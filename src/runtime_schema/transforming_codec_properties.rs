//! Numbers through the codecs whose contract is a JAQ transformation.
//!
//! Layer: test harness.
//!
//! - **Owns.** The JAQ-native and protobuf codecs of one schema of numbers, the round trip of
//!   every row of a generated batch through one of them, and the reproducers of the floats a
//!   best-effort decimal reader moved to a neighbour.
//! - **Depends on.** The generated batches and their logical oracle, the codec Models and the
//!   compiled codecs' row encoder and payload decoder.
//! - **Must not know.** Ingestors, emitters, connectors or the ingest group that owns a builder.
//!
//! A program decides the shape of everything these codecs carry, so no property holds them to
//! every value of every schema. Their numbers pass through unchanged programs unchanged, and that
//! is held here: every codec below runs the identity program in both directions, except that the
//! protobuf codec restores on ingestion the defaults a protobuf message does not carry. `XML`
//! holds text a program has to parse into numbers, so it has no identity codec.

use arrow_array::{ArrayRef, Float32Array, Float64Array, Int64Array, ListArray, RecordBatch};
use arrow_buffer::OffsetBuffer;
use arrow_schema::DataType;
use meticulous::ResultExt as _;
use nervix_arbitrary::{Arbitrary, Domain};
use nervix_models::{
    CodecJaqFormat, CodecJaqTransformations, CodecName, CodecProtobufConfig, CreateSchema,
    FieldName, ParseAsType, ResolvedCodecWireFormat, ResourceName, SchemaField, SchemaName,
};
use nervix_primitives::sync::{Arc, StdArc};
use prost_reflect::MessageDescriptor;
use prost_types::{
    DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
    field_descriptor_proto::{Label, Type},
};
use rstest::rstest;

use super::{
    CompiledCodec, JsonDecoder, ProtobufCodecDescriptors, ProtobufDescriptorPool,
    compile_codec_spec_with_protobuf, decode_with_codec,
};
use crate::runtime_schema::generated_batches::{
    BatchRows, GeneratedDomain, GeneratedSchema, LogicalValue,
};

/// The bytes one case reads its codec and rows from.
const CASE_BYTES: usize = 1024;

/// The protobuf message the numbers schema is carried as.
const NUMBERS_MESSAGE: &str = "nervix.test.Numbers";

/// The ingestion program of the protobuf codec. A protobuf message does not carry a singular field
/// that holds its default or a repeated field that holds nothing, so its JSON view lacks them and
/// the program restores each.
const RESTORING_DEFAULTS: &str = "{wide: (.wide // 0), narrow: (.narrow // 0), whole: (.whole // \
                                  0), wides: (.wides // []), narrows: (.narrows // [])}";

/// The bits of a negative zero of each float width.
const NEGATIVE_ZERO_F64: u64 = 0x8000_0000_0000_0000;
const NEGATIVE_ZERO_F32: u32 = 0x8000_0000;

/// A codec whose contract is a JAQ transformation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransformingFormat {
    /// A JAQ-native codec of a format that holds numbers.
    Native(CodecJaqFormat),
    /// A protobuf codec of [`NUMBERS_MESSAGE`].
    Protobuf,
}

impl TransformingFormat {
    const ALL: [Self; 5] = [
        Self::Native(CodecJaqFormat::Json),
        Self::Native(CodecJaqFormat::Yaml),
        Self::Native(CodecJaqFormat::Toml),
        Self::Native(CodecJaqFormat::Cbor),
        Self::Protobuf,
    ];

    /// The codec of this format over `numbers`.
    fn codec(self, numbers: &Numbers) -> Arc<CompiledCodec> {
        let name = CodecName::parse("numbers_codec").assured("the codec name is a literal");
        let schema = numbers.schema.compiled.clone();
        match self {
            Self::Native(format) => {
                let transformations = CodecJaqTransformations {
                    on_ingestion: Some(String::from(".")),
                    on_emitting: Some(String::from(".")),
                    on_emitting_batch: None,
                };
                let wire_format = ResolvedCodecWireFormat::JaqNative {
                    format,
                    transformations: &transformations,
                };
                compile_codec_spec_with_protobuf(&name, &[], schema, wire_format, None)
                    .assured("an identity JAQ-native codec compiles")
            }
            Self::Protobuf => {
                let config = CodecProtobufConfig {
                    resource: ResourceName::parse("proto_bundle")
                        .assured("the resource name is a literal"),
                    resource_version: 1,
                    config: Vec::new(),
                    message: String::from(NUMBERS_MESSAGE),
                    batch_message: None,
                    transformations: CodecJaqTransformations {
                        on_ingestion: Some(String::from(RESTORING_DEFAULTS)),
                        on_emitting: Some(String::from(".")),
                        on_emitting_batch: None,
                    },
                };
                let descriptors = ProtobufCodecDescriptors {
                    message: numbers.message.clone(),
                    batch_message: None,
                };
                compile_codec_spec_with_protobuf(
                    &name,
                    &[],
                    schema,
                    ResolvedCodecWireFormat::Protobuf(&config),
                    Some(descriptors),
                )
                .assured("a protobuf codec of the numbers message compiles")
            }
        }
    }

    /// The rows a decoder of this format reads back for `rows`. They are the rows themselves,
    /// except that a protobuf message does not carry a singular field that holds its default, and
    /// a zero of either sign is that default: a negative zero of `wide` or `narrow` reads back as
    /// the positive zero the ingestion program restores. An element of a list keeps its sign.
    fn carried(self, rows: Vec<Vec<Option<LogicalValue>>>) -> Vec<Vec<Option<LogicalValue>>> {
        if self != Self::Protobuf {
            return rows;
        }
        let mut carried = Vec::with_capacity(rows.len());
        for row in rows {
            let mut cells = Vec::with_capacity(row.len());
            for cell in row {
                let cell = match cell {
                    Some(LogicalValue::F64(NEGATIVE_ZERO_F64)) => Some(LogicalValue::F64(0)),
                    Some(LogicalValue::F32(NEGATIVE_ZERO_F32)) => Some(LogicalValue::F32(0)),
                    other => other,
                };
                cells.push(cell);
            }
            carried.push(cells);
        }
        carried
    }
}

/// One transforming format and its codec over the numbers schema.
#[derive(Debug, Clone)]
struct FormatCase {
    format: TransformingFormat,
    codec: Arc<CompiledCodec>,
}

/// The schema of numbers every codec here carries, and the protobuf message of the same fields.
struct Numbers {
    schema: GeneratedSchema,
    message: MessageDescriptor,
}

impl Numbers {
    fn new() -> Self {
        let fields = vec![
            Self::field("wide", ParseAsType::F64),
            Self::field("narrow", ParseAsType::F32),
            Self::field("whole", ParseAsType::I64),
            Self::field(
                "wides",
                ParseAsType::Vec {
                    element: Box::new(ParseAsType::F64),
                },
            ),
            Self::field(
                "narrows",
                ParseAsType::Vec {
                    element: Box::new(ParseAsType::F32),
                },
            ),
        ];
        let schema = GeneratedSchema::new(CreateSchema {
            name: SchemaName::parse("numbers").assured("the schema name is a literal name"),
            fields,
        });
        Self {
            schema,
            message: Self::message(),
        }
    }

    /// A required field every reader may see.
    fn field(name: &str, ty: ParseAsType) -> SchemaField {
        SchemaField {
            name: FieldName::parse(name).assured("the field name is a literal name"),
            ty,
            optional: false,
            sensitive: false,
        }
    }

    /// The proto3 message of the schema's fields under the same names.
    fn message() -> MessageDescriptor {
        let fields = vec![
            Self::message_field("wide", 1, Label::Optional, Type::Double),
            Self::message_field("narrow", 2, Label::Optional, Type::Float),
            Self::message_field("whole", 3, Label::Optional, Type::Int64),
            Self::message_field("wides", 4, Label::Repeated, Type::Double),
            Self::message_field("narrows", 5, Label::Repeated, Type::Float),
        ];
        let message = DescriptorProto {
            name: Some(String::from("Numbers")),
            field: fields,
            ..Default::default()
        };
        let file = FileDescriptorProto {
            name: Some(String::from("numbers.proto")),
            package: Some(String::from("nervix.test")),
            message_type: vec![message],
            syntax: Some(String::from("proto3")),
            ..Default::default()
        };
        ProtobufDescriptorPool::from_file_descriptor_set(FileDescriptorSet { file: vec![file] })
            .assured("the descriptor set declares one valid message")
            .message(NUMBERS_MESSAGE)
            .assured("the pool holds the message it was built from")
    }

    fn message_field(name: &str, number: i32, label: Label, ty: Type) -> FieldDescriptorProto {
        FieldDescriptorProto {
            name: Some(String::from(name)),
            number: Some(number),
            label: Some(label.into()),
            r#type: Some(ty.into()),
            json_name: Some(String::from(name)),
            ..Default::default()
        }
    }

    /// Three rows of floats whose shortest decimal a best-effort decimal reader reads as a
    /// neighbouring float, beside integers at their bounds and a negative zero inside each list.
    fn neighboured_rows(&self) -> RecordBatch {
        let wide: ArrayRef = StdArc::new(Float64Array::from(vec![
            1.4000000000000001,
            0.9999999999999999,
            90.33333333333333,
        ]));
        let narrow: ArrayRef = StdArc::new(Float32Array::from(vec![1.1, 7.038531e-26, 0.1]));
        let whole: ArrayRef = StdArc::new(Int64Array::from(vec![i64::MIN, -1, i64::MAX]));
        let wides = Self::list(
            &ParseAsType::F64,
            StdArc::new(Float64Array::from(vec![
                2.0030397744267762e-253,
                -0.0,
                1.4000000000000001,
            ])),
            [2, 0, 1],
        );
        let narrows = Self::list(
            &ParseAsType::F32,
            StdArc::new(Float32Array::from(vec![1.1, -0.0, 7.038531e-26])),
            [2, 0, 1],
        );
        RecordBatch::try_new(
            self.schema.compiled.arrow_schema(),
            vec![wide, narrow, whole, wides, narrows],
        )
        .assured("the columns hold one value of their declared type per row")
    }

    /// A list column of `element` values whose rows own `lengths` of `values` in order.
    fn list(element: &ParseAsType, values: ArrayRef, lengths: [usize; 3]) -> ArrayRef {
        let ty = ParseAsType::Vec {
            element: Box::new(element.clone()),
        };
        let DataType::List(field) = ty.arrow_data_type() else {
            unreachable!("a VEC is carried as an Arrow list")
        };
        let list = ListArray::try_new(field, OffsetBuffer::from_lengths(lengths), values, None)
            .assured("the lengths address every value in order");
        StdArc::new(list)
    }
}

/// Asserts that every row of `rows` reads back from the payload `codec` writes for it as the row
/// its format carries: the same schema, the same number of rows, every integer with its value and
/// every float with its bits.
fn assert_round_trip(
    format: TransformingFormat,
    codec: &CompiledCodec,
    numbers: &Numbers,
    rows: &RecordBatch,
) {
    let batch = numbers.schema.runtime_batch(rows.clone());
    let encoder = codec
        .batch_encoder(&batch)
        .assured("a batch of the codec's schema opens an encoder");
    let mut builder = codec.schema().batch_builder(rows.num_rows());
    let mut decoder = JsonDecoder::default();
    for row in 0..rows.num_rows() {
        let mut payload = encoder.next_payload();
        encoder
            .encode_row_into(row, &mut payload)
            .assured("every number of the codec's domain encodes");
        let messages = decode_with_codec(codec, &payload, &mut decoder, &mut builder)
            .assured("a payload the codec wrote decodes");
        assert_eq!(messages, 1, "one payload decodes into one message");
    }
    let decoded = builder
        .finish()
        .assured("the builder holds whole rows after every payload");
    assert_eq!(
        decoded.batch().schema(),
        rows.schema(),
        "the schema is preserved"
    );
    assert_eq!(
        LogicalValue::rows(decoded.batch()),
        format.carried(LogicalValue::rows(rows)),
        "every number is preserved through {format:?}"
    );
}

#[rstest]
#[case::json(TransformingFormat::Native(CodecJaqFormat::Json))]
#[case::yaml(TransformingFormat::Native(CodecJaqFormat::Yaml))]
#[case::toml(TransformingFormat::Native(CodecJaqFormat::Toml))]
#[case::cbor(TransformingFormat::Native(CodecJaqFormat::Cbor))]
#[case::protobuf(TransformingFormat::Protobuf)]
fn a_float_keeps_its_bits_through_a_transforming_codec(#[case] format: TransformingFormat) {
    let numbers = Numbers::new();
    let codec = format.codec(&numbers);

    assert_round_trip(format, &codec, &numbers, &numbers.neighboured_rows());
}

/// Every number a JAQ-native or protobuf codec encodes through unchanged programs decodes back to
/// itself: a finite `F64` and `F32` of any bit pattern keeps its bits, alone and as an element of
/// a list, and an `I64` keeps its value, through `JSON`, `YAML`, `TOML`, `CBOR` and a protobuf
/// message. The one projection is protobuf's: a singular field that holds a zero of either sign is
/// its default, which the message does not carry, so it reads back as positive zero.
#[test]
fn bolero_transforming_codecs_decode_every_number_they_encode() {
    let numbers = Numbers::new();
    let cases = TransformingFormat::ALL.map(|format| FormatCase {
        format,
        codec: format.codec(&numbers),
    });
    bolero::check!()
        .with_iterations(256)
        .with_max_len(CASE_BYTES)
        .for_each(|input| {
            let mut arbitrary = Arbitrary::new(input, Domain::Vocabulary);
            // The format and the row count are read before the rows, so an ordinary run's few
            // bytes reach every format and leave the rest to the values of the first columns.
            let case = arbitrary.entropy().pick(cases.clone());
            let shape = BatchRows::draw(arbitrary.entropy());
            let rows = GeneratedDomain::Json.batch_of(&mut arbitrary, &numbers.schema, shape);

            assert_round_trip(case.format, &case.codec, &numbers, &rows);
        });
}
