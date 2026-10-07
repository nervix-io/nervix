//! Hand-built Arrow IPC streams that declare what Arrow's Rust writer never writes.
//!
//! Layer: test harness.
//!
//! - **Owns.** One stream for each declaration Arrow's stream reader panics on, or aborts the
//!   process on, instead of refusing, and one valid stream for each freedom of the Arrow format
//!   that another Arrow writer takes and Arrow's Rust writer does not. Every stream is one schema
//!   message of one field, at most one record batch message and the end-of-stream marker, built
//!   message by message so a test controls every declaration.
//! - **Depends on.** Arrow's IPC message definitions and the FlatBuffers builder.
//! - **Must not know.** The decoders a test hands a stream to.

use arrow_array::{ArrayRef, Int64Array, StringArray};
use arrow_ipc::{
    Buffer, DictionaryEncoding, DictionaryEncodingArgs, Endianness, Field, FieldArgs, FieldNode,
    FixedSizeList, FixedSizeListArgs, Int, IntArgs, List, ListArgs, Message, MessageArgs,
    MessageHeader, MetadataVersion, RecordBatch, RecordBatchArgs, Schema, SchemaArgs, Type, Utf8,
    Utf8Args,
};
use flatbuffers::{FlatBufferBuilder, ForwardsUOffset, UnionWIPOffset, Vector, WIPOffset};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{CreateSchema, FieldName, ParseAsType, SchemaField, SchemaName};
use nervix_primitives::sync::StdArc;

use super::{
    generated_batches::GeneratedSchema,
    ipc_stream::{IpcStreamError, UnsupportedFieldKind},
};

/// The end-of-stream marker: the continuation marker and a zero metadata length.
pub(crate) const END_OF_STREAM: [u8; 8] = [0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0];

/// The schema a receiver of a defective stream expects: one required `I64` field. Every defective
/// stream is refused before its schema is compared with this one.
pub(crate) fn receiver_schema() -> GeneratedSchema {
    single_field_schema(ParseAsType::I64, false)
}

/// A schema of one field named `value`.
fn single_field_schema(ty: ParseAsType, optional: bool) -> GeneratedSchema {
    GeneratedSchema::new(CreateSchema {
        name: SchemaName::parse("single").assured("a literal schema name"),
        fields: vec![SchemaField {
            name: FieldName::parse("value").assured("a literal field name"),
            ty,
            optional,
            sensitive: false,
        }],
    })
}

/// What one crafted stream declares that Arrow's reader panics on, or allocates for until the
/// process aborts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StreamDefect {
    /// A values buffer reaching past the eight-byte body its record batch message declares.
    BufferPastBody,
    /// An integer field seven bits wide.
    IntegerOfSevenBits,
    /// A list field without the child field its elements have.
    ListWithoutChild,
    /// A dictionary-encoded field.
    DictionaryEncoded,
    /// A schema without its field list.
    SchemaWithoutFields,
    /// A field node counting a null that its empty validity bitmap cannot hold.
    ValidityShorterThanRows,
    /// A text field whose offsets buffer ends inside an offset.
    OffsetsCutInsideAnOffset,
    /// A record batch declaring variadic buffer counts.
    VariadicBufferCounts,
    /// A fixed-size list whose length times its size overflows.
    FixedSizeListTooLongToCount,
    /// A record batch message declaring a body of 2^60 bytes before the eight that follow it, which
    /// Arrow's reader allocates before it reads them.
    BodyLongerThanStream,
}

/// What one crafted stream declares that the Arrow format allows, that Arrow's C++, Go, JavaScript
/// or Java writer writes, and that Arrow's Rust writer never does. Each is a valid stream of one
/// field and one record batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WriterFreedom {
    /// Three required integers without a validity bitmap, as the C++, Go and JavaScript writers
    /// write a column that holds no null. Arrow's Rust writer writes a bitmap of set bits.
    ValidityOmitted,
    /// Three optional integers, the second null, under a validity bitmap eight bytes long, as the
    /// Go and JavaScript writers size it. Arrow's Rust writer writes the one byte three rows need.
    ValidityLongerThanRows,
    /// One text value behind four offsets, as the JavaScript writer leaves the offsets of a column
    /// it built. Arrow's Rust writer writes one offset more than the column has rows.
    OffsetsLongerThanRows,
    /// A text column of no rows without any offset, as the Java writer writes it. Arrow's Rust
    /// writer writes the one offset an empty column starts at.
    OffsetsEmptyWithoutRows,
}

/// What one crafted field declares.
struct CraftedField<'a> {
    type_type: Type,
    type_: WIPOffset<UnionWIPOffset>,
    children: WIPOffset<Vector<'a, ForwardsUOffset<Field<'a>>>>,
    dictionary: Option<WIPOffset<DictionaryEncoding<'a>>>,
}

impl<'a> CraftedField<'a> {
    /// A signed integer type `bits` wide.
    fn integer(builder: &mut FlatBufferBuilder<'a>, bits: i32) -> WIPOffset<UnionWIPOffset> {
        Int::create(
            builder,
            &IntArgs {
                bitWidth: bits,
                is_signed: true,
            },
        )
        .as_union_value()
    }

    fn no_children(
        builder: &mut FlatBufferBuilder<'a>,
    ) -> WIPOffset<Vector<'a, ForwardsUOffset<Field<'a>>>> {
        builder.create_vector::<ForwardsUOffset<Field<'a>>>(&[])
    }

    /// The field as a schema declares it, named `name`.
    fn declare(
        self,
        builder: &mut FlatBufferBuilder<'a>,
        name: &str,
        nullable: bool,
    ) -> WIPOffset<Field<'a>> {
        let name = builder.create_string(name);
        Field::create(
            builder,
            &FieldArgs {
                name: Some(name),
                nullable,
                type_type: self.type_type,
                type_: Some(self.type_),
                dictionary: self.dictionary,
                children: Some(self.children),
                custom_metadata: None,
            },
        )
    }
}

/// What one crafted record batch message declares, and the body that follows it.
struct CraftedBatch {
    /// The rows the record batch declares.
    rows: i64,
    nodes: Vec<FieldNode>,
    buffers: Vec<Buffer>,
    variadic_buffer_counts: Option<Vec<i64>>,
    /// The body length the message declares.
    declared_body: i64,
    /// The bytes that follow the message as its body.
    body: Vec<u8>,
}

impl CraftedBatch {
    /// The record batch message declaring what this batch holds.
    fn message(&self) -> Vec<u8> {
        let mut builder = FlatBufferBuilder::new();
        let nodes = builder.create_vector(&self.nodes);
        let buffers = builder.create_vector(&self.buffers);
        let variadic_buffer_counts = self
            .variadic_buffer_counts
            .as_ref()
            .map(|counts| builder.create_vector(counts));
        let record_batch = RecordBatch::create(
            &mut builder,
            &RecordBatchArgs {
                length: self.rows,
                nodes: Some(nodes),
                buffers: Some(buffers),
                compression: None,
                variadicBufferCounts: variadic_buffer_counts,
            },
        );
        let message = Message::create(
            &mut builder,
            &MessageArgs {
                version: MetadataVersion::V5,
                header_type: MessageHeader::RecordBatch,
                header: Some(record_batch.as_union_value()),
                bodyLength: self.declared_body,
                custom_metadata: None,
            },
        );
        builder.finish(message, None);
        builder.finished_data().to_vec()
    }
}

/// The messages of one crafted stream, framed as a writer frames them.
struct CraftedStream {
    bytes: Vec<u8>,
}

impl CraftedStream {
    fn new() -> Self {
        Self { bytes: Vec::new() }
    }

    /// A schema message declaring `fields`, or no field list at all, finished in the builder the
    /// fields were declared in.
    fn schema_message<'a>(
        mut builder: FlatBufferBuilder<'a>,
        fields: Option<WIPOffset<Vector<'a, ForwardsUOffset<Field<'a>>>>>,
    ) -> Vec<u8> {
        let schema = Schema::create(
            &mut builder,
            &SchemaArgs {
                endianness: Endianness::Little,
                fields,
                custom_metadata: None,
                features: None,
            },
        );
        let message = Message::create(
            &mut builder,
            &MessageArgs {
                version: MetadataVersion::V5,
                header_type: MessageHeader::Schema,
                header: Some(schema.as_union_value()),
                bodyLength: 0,
                custom_metadata: None,
            },
        );
        builder.finish(message, None);
        builder.finished_data().to_vec()
    }

    /// Appends one message: the continuation marker, the padded metadata length, the metadata, its
    /// padding and `body`.
    fn frame(&mut self, metadata: &[u8], body: &[u8]) {
        let padded = metadata
            .len()
            .div_ceil(8)
            .checked_mul(8)
            .assured("a small message pads to eight bytes");
        self.bytes.extend_from_slice(&[0xff; 4]);
        self.bytes.extend_from_slice(
            &i32::try_from(padded)
                .assured("a small message length fits i32")
                .to_le_bytes(),
        );
        self.bytes.extend_from_slice(metadata);
        let padding = padded
            .checked_sub(metadata.len())
            .verified("the padded length is the metadata length rounded up");
        let end = self
            .bytes
            .len()
            .checked_add(padding)
            .assured("a small message fits in memory");
        self.bytes.resize(end, 0);
        self.bytes.extend_from_slice(body);
    }

    /// The stream, closed with the end-of-stream marker.
    fn finish(mut self) -> Vec<u8> {
        self.bytes.extend_from_slice(&END_OF_STREAM);
        self.bytes
    }
}

impl StreamDefect {
    /// What the scan refuses the stream for.
    pub(crate) fn refusal(self) -> IpcStreamError {
        match self {
            Self::BufferPastBody => IpcStreamError::BufferOutsideBody { buffer: 1 },
            Self::IntegerOfSevenBits => IpcStreamError::UnsupportedField {
                field: 0,
                kind: UnsupportedFieldKind::IntegerWidth { bits: 7 },
            },
            Self::ListWithoutChild => IpcStreamError::UnsupportedField {
                field: 0,
                kind: UnsupportedFieldKind::ListChildren { children: 0 },
            },
            Self::DictionaryEncoded => IpcStreamError::UnsupportedField {
                field: 0,
                kind: UnsupportedFieldKind::Dictionary,
            },
            Self::SchemaWithoutFields => IpcStreamError::MissingFields,
            Self::ValidityShorterThanRows => IpcStreamError::ValidityTooShort { buffer: 0 },
            Self::OffsetsCutInsideAnOffset => IpcStreamError::OffsetsNotWhole { buffer: 1 },
            Self::VariadicBufferCounts => IpcStreamError::VariadicBuffers,
            Self::FixedSizeListTooLongToCount => IpcStreamError::NodeLength { node: 0 },
            Self::BodyLongerThanStream => IpcStreamError::Truncated,
        }
    }

    /// The whole stream: its schema message, its record batch message when the defect is one of a
    /// record batch, and the end-of-stream marker.
    pub(crate) fn stream(self) -> Vec<u8> {
        let mut stream = CraftedStream::new();
        stream.frame(&self.schema_message(), &[]);
        if let Some(batch) = self.batch() {
            stream.frame(&batch.message(), &batch.body);
        }
        stream.finish()
    }

    fn schema_message(self) -> Vec<u8> {
        let mut builder = FlatBufferBuilder::new();
        let fields = match self {
            Self::SchemaWithoutFields => None,
            Self::BufferPastBody
            | Self::IntegerOfSevenBits
            | Self::ListWithoutChild
            | Self::DictionaryEncoded
            | Self::ValidityShorterThanRows
            | Self::OffsetsCutInsideAnOffset
            | Self::VariadicBufferCounts
            | Self::FixedSizeListTooLongToCount
            | Self::BodyLongerThanStream => {
                let field = self.field(&mut builder);
                Some(builder.create_vector(&[field]))
            }
        };
        CraftedStream::schema_message(builder, fields)
    }

    /// The stream's one field, required and named `value` as the receiver's schema declares it.
    fn field<'a>(self, builder: &mut FlatBufferBuilder<'a>) -> WIPOffset<Field<'a>> {
        let crafted = match self {
            Self::IntegerOfSevenBits => CraftedField {
                type_type: Type::Int,
                type_: CraftedField::integer(builder, 7),
                children: CraftedField::no_children(builder),
                dictionary: None,
            },
            Self::ListWithoutChild => CraftedField {
                type_type: Type::List,
                type_: List::create(builder, &ListArgs {}).as_union_value(),
                children: CraftedField::no_children(builder),
                dictionary: None,
            },
            Self::DictionaryEncoded => {
                let dictionary = DictionaryEncoding::create(
                    builder,
                    &DictionaryEncodingArgs {
                        id: 0,
                        indexType: None,
                        ..DictionaryEncodingArgs::default()
                    },
                );
                CraftedField {
                    type_type: Type::Int,
                    type_: CraftedField::integer(builder, 64),
                    children: CraftedField::no_children(builder),
                    dictionary: Some(dictionary),
                }
            }
            Self::OffsetsCutInsideAnOffset => CraftedField {
                type_type: Type::Utf8,
                type_: Utf8::create(builder, &Utf8Args {}).as_union_value(),
                children: CraftedField::no_children(builder),
                dictionary: None,
            },
            Self::FixedSizeListTooLongToCount => {
                let element_type = CraftedField::integer(builder, 64);
                let element_children = CraftedField::no_children(builder);
                let element_name = builder.create_string("item");
                let element = Field::create(
                    builder,
                    &FieldArgs {
                        name: Some(element_name),
                        nullable: false,
                        type_type: Type::Int,
                        type_: Some(element_type),
                        dictionary: None,
                        children: Some(element_children),
                        custom_metadata: None,
                    },
                );
                let list =
                    FixedSizeList::create(builder, &FixedSizeListArgs { listSize: i32::MAX });
                CraftedField {
                    type_type: Type::FixedSizeList,
                    type_: list.as_union_value(),
                    children: builder.create_vector(&[element]),
                    dictionary: None,
                }
            }
            Self::BufferPastBody
            | Self::SchemaWithoutFields
            | Self::ValidityShorterThanRows
            | Self::VariadicBufferCounts
            | Self::BodyLongerThanStream => CraftedField {
                type_type: Type::Int,
                type_: CraftedField::integer(builder, 64),
                children: CraftedField::no_children(builder),
                dictionary: None,
            },
        };
        crafted.declare(builder, "value", false)
    }

    /// The record batch message the stream carries, or `None` when the schema message alone holds
    /// the defect.
    fn batch(self) -> Option<CraftedBatch> {
        let batch = match self {
            Self::IntegerOfSevenBits
            | Self::ListWithoutChild
            | Self::DictionaryEncoded
            | Self::SchemaWithoutFields => return None,
            Self::BufferPastBody => CraftedBatch {
                rows: 1,
                nodes: vec![FieldNode::new(1, 0)],
                buffers: vec![Buffer::new(0, 0), Buffer::new(0, 1 << 40)],
                variadic_buffer_counts: None,
                declared_body: 8,
                body: vec![0; 8],
            },
            Self::ValidityShorterThanRows => CraftedBatch {
                rows: 1,
                nodes: vec![FieldNode::new(1, 1)],
                buffers: vec![Buffer::new(0, 0), Buffer::new(0, 8)],
                variadic_buffer_counts: None,
                declared_body: 8,
                body: vec![0; 8],
            },
            // Nine bytes hold the two offsets one value needs and one byte more.
            Self::OffsetsCutInsideAnOffset => CraftedBatch {
                rows: 1,
                nodes: vec![FieldNode::new(1, 0)],
                buffers: vec![Buffer::new(0, 0), Buffer::new(0, 9), Buffer::new(16, 0)],
                variadic_buffer_counts: None,
                declared_body: 16,
                body: vec![0; 16],
            },
            Self::VariadicBufferCounts => CraftedBatch {
                rows: 1,
                nodes: vec![FieldNode::new(1, 0)],
                buffers: vec![Buffer::new(0, 0), Buffer::new(0, 8)],
                variadic_buffer_counts: Some(vec![1]),
                declared_body: 8,
                body: vec![0; 8],
            },
            // The list's length times its size of `i32::MAX` does not fit in 64 bits.
            Self::FixedSizeListTooLongToCount => CraftedBatch {
                rows: 1,
                nodes: vec![FieldNode::new(1 << 40, 0), FieldNode::new(0, 0)],
                buffers: vec![Buffer::new(0, 0), Buffer::new(0, 0), Buffer::new(0, 0)],
                variadic_buffer_counts: None,
                declared_body: 0,
                body: Vec::new(),
            },
            Self::BodyLongerThanStream => CraftedBatch {
                rows: 1,
                nodes: vec![FieldNode::new(1, 0)],
                buffers: vec![Buffer::new(0, 0), Buffer::new(0, 8)],
                variadic_buffer_counts: None,
                declared_body: 1 << 60,
                body: vec![0; 8],
            },
        };
        Some(batch)
    }
}

impl WriterFreedom {
    /// The schema a receiver of the stream expects: its one field, named `value`.
    pub(crate) fn receiver_schema(self) -> GeneratedSchema {
        match self {
            Self::ValidityOmitted => single_field_schema(ParseAsType::I64, false),
            Self::ValidityLongerThanRows => single_field_schema(ParseAsType::I64, true),
            Self::OffsetsLongerThanRows | Self::OffsetsEmptyWithoutRows => {
                single_field_schema(ParseAsType::String, false)
            }
        }
    }

    /// Whether the stream's field admits a null.
    pub(crate) fn nullable(self) -> bool {
        match self {
            Self::ValidityLongerThanRows => true,
            Self::ValidityOmitted | Self::OffsetsLongerThanRows | Self::OffsetsEmptyWithoutRows => {
                false
            }
        }
    }

    /// The column the stream holds.
    pub(crate) fn column(self) -> ArrayRef {
        match self {
            Self::ValidityOmitted => StdArc::new(Int64Array::from(vec![1, -2, i64::MAX])),
            Self::ValidityLongerThanRows => {
                StdArc::new(Int64Array::from(vec![Some(1), None, Some(3)]))
            }
            Self::OffsetsLongerThanRows => StdArc::new(StringArray::from(vec!["hi"])),
            Self::OffsetsEmptyWithoutRows => StdArc::new(StringArray::from(Vec::<&str>::new())),
        }
    }

    /// The whole stream, its one field named `field_name`: the schema message, the record batch
    /// message and the end-of-stream marker.
    pub(crate) fn stream(self, field_name: &str) -> Vec<u8> {
        let batch = self.batch();
        let mut stream = CraftedStream::new();
        stream.frame(&self.schema_message(field_name), &[]);
        stream.frame(&batch.message(), &batch.body);
        stream.finish()
    }

    fn schema_message(self, field_name: &str) -> Vec<u8> {
        let mut builder = FlatBufferBuilder::new();
        let crafted = match self {
            Self::ValidityOmitted | Self::ValidityLongerThanRows => CraftedField {
                type_type: Type::Int,
                type_: CraftedField::integer(&mut builder, 64),
                children: CraftedField::no_children(&mut builder),
                dictionary: None,
            },
            Self::OffsetsLongerThanRows | Self::OffsetsEmptyWithoutRows => CraftedField {
                type_type: Type::Utf8,
                type_: Utf8::create(&mut builder, &Utf8Args {}).as_union_value(),
                children: CraftedField::no_children(&mut builder),
                dictionary: None,
            },
        };
        let field = crafted.declare(&mut builder, field_name, self.nullable());
        let fields = builder.create_vector(&[field]);
        CraftedStream::schema_message(builder, Some(fields))
    }

    /// The record batch the stream carries, its buffers as the freedom's writers lay them out.
    fn batch(self) -> CraftedBatch {
        match self {
            // No validity bitmap, then three values.
            Self::ValidityOmitted => {
                let mut body = Vec::new();
                for value in [1_i64, -2, i64::MAX] {
                    body.extend_from_slice(&value.to_le_bytes());
                }
                CraftedBatch {
                    rows: 3,
                    nodes: vec![FieldNode::new(3, 0)],
                    buffers: vec![Buffer::new(0, 0), Buffer::new(0, 24)],
                    variadic_buffer_counts: None,
                    declared_body: 24,
                    body,
                }
            }
            // Eight bytes of validity whose first and third bits are set, then three values of
            // which the second is not read.
            Self::ValidityLongerThanRows => {
                let mut body = vec![0b0000_0101, 0, 0, 0, 0, 0, 0, 0];
                for value in [1_i64, 0, 3] {
                    body.extend_from_slice(&value.to_le_bytes());
                }
                CraftedBatch {
                    rows: 3,
                    nodes: vec![FieldNode::new(3, 1)],
                    buffers: vec![Buffer::new(0, 8), Buffer::new(8, 24)],
                    variadic_buffer_counts: None,
                    declared_body: 32,
                    body,
                }
            }
            // Four offsets, of which one row reads the first two, then the two bytes they delimit
            // and the padding that ends the body on an eight-byte boundary.
            Self::OffsetsLongerThanRows => {
                let mut body = Vec::new();
                for offset in [0_i32, 2, 2, 2] {
                    body.extend_from_slice(&offset.to_le_bytes());
                }
                body.extend_from_slice(b"hi");
                body.resize(24, 0);
                CraftedBatch {
                    rows: 1,
                    nodes: vec![FieldNode::new(1, 0)],
                    buffers: vec![Buffer::new(0, 0), Buffer::new(0, 16), Buffer::new(16, 2)],
                    variadic_buffer_counts: None,
                    declared_body: 24,
                    body,
                }
            }
            Self::OffsetsEmptyWithoutRows => CraftedBatch {
                rows: 0,
                nodes: vec![FieldNode::new(0, 0)],
                buffers: vec![Buffer::new(0, 0), Buffer::new(0, 0), Buffer::new(0, 0)],
                variadic_buffer_counts: None,
                declared_body: 0,
                body: Vec::new(),
            },
        }
    }
}
