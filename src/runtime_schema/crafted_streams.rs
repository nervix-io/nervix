//! Hand-built Arrow IPC streams that declare what Arrow's writer never writes.
//!
//! Layer: test harness.
//!
//! - **Owns.** One stream for each declaration Arrow's stream reader panics on, or aborts the
//!   process on, instead of refusing: every stream is one schema message of one field, at most one
//!   record batch message and the end-of-stream marker, built message by message so a test controls
//!   every declaration.
//! - **Depends on.** Arrow's IPC message definitions and the FlatBuffers builder.
//! - **Must not know.** The decoders a test hands a stream to.

use arrow_ipc::{
    Buffer, DictionaryEncoding, DictionaryEncodingArgs, Endianness, Field, FieldArgs, FieldNode,
    FixedSizeList, FixedSizeListArgs, Int, IntArgs, List, ListArgs, Message, MessageArgs,
    MessageHeader, MetadataVersion, RecordBatch, RecordBatchArgs, Schema, SchemaArgs, Type, Utf8,
    Utf8Args,
};
use flatbuffers::{FlatBufferBuilder, ForwardsUOffset, UnionWIPOffset, Vector, WIPOffset};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{CreateSchema, FieldName, ParseAsType, SchemaField, SchemaName};

use super::{
    generated_batches::GeneratedSchema,
    ipc_stream::{IpcStreamError, UnsupportedFieldKind},
};

/// The end-of-stream marker: the continuation marker and a zero metadata length.
pub(crate) const END_OF_STREAM: [u8; 8] = [0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0];

/// The schema a receiver of a crafted stream expects: one required `I64` field. Every crafted
/// stream is refused before its schema is compared with this one.
pub(crate) fn receiver_schema() -> GeneratedSchema {
    GeneratedSchema::new(CreateSchema {
        name: SchemaName::parse("single").assured("a literal schema name"),
        fields: vec![SchemaField {
            name: FieldName::parse("value").assured("a literal field name"),
            ty: ParseAsType::I64,
            optional: false,
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

/// What one crafted field declares.
struct CraftedField<'a> {
    type_type: Type,
    type_: WIPOffset<UnionWIPOffset>,
    children: WIPOffset<Vector<'a, ForwardsUOffset<Field<'a>>>>,
    dictionary: Option<WIPOffset<DictionaryEncoding<'a>>>,
}

/// What one crafted record batch message declares.
struct CraftedBatch {
    nodes: Vec<FieldNode>,
    buffers: Vec<Buffer>,
    variadic_buffer_counts: Option<Vec<i64>>,
    /// The body length the message declares.
    declared_body: i64,
    /// How many zero bytes follow the message as its body.
    body: usize,
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
        let mut stream = Vec::new();
        Self::frame(&mut stream, &self.schema_message(), 0);
        if let Some(batch) = self.batch() {
            Self::frame(&mut stream, &Self::batch_message(&batch), batch.body);
        }
        stream.extend_from_slice(&END_OF_STREAM);
        stream
    }

    /// Appends one message: the continuation marker, the padded metadata length, the metadata, its
    /// padding and a body of `body` zero bytes.
    fn frame(stream: &mut Vec<u8>, metadata: &[u8], body: usize) {
        let padded = metadata
            .len()
            .div_ceil(8)
            .checked_mul(8)
            .assured("a small message pads to eight bytes");
        stream.extend_from_slice(&[0xff; 4]);
        stream.extend_from_slice(
            &i32::try_from(padded)
                .assured("a small message length fits i32")
                .to_le_bytes(),
        );
        stream.extend_from_slice(metadata);
        let padding = padded
            .checked_sub(metadata.len())
            .verified("the padded length is the metadata length rounded up");
        let end = stream
            .len()
            .checked_add(padding)
            .assured("a small message fits in memory");
        let end = end.checked_add(body).assured("a small body fits in memory");
        stream.resize(end, 0);
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

    /// The stream's one field, required and named `value` as the receiver's schema declares it.
    fn field<'a>(self, builder: &mut FlatBufferBuilder<'a>) -> WIPOffset<Field<'a>> {
        let crafted = match self {
            Self::IntegerOfSevenBits => CraftedField {
                type_type: Type::Int,
                type_: Self::integer(builder, 7),
                children: Self::no_children(builder),
                dictionary: None,
            },
            Self::ListWithoutChild => CraftedField {
                type_type: Type::List,
                type_: List::create(builder, &ListArgs {}).as_union_value(),
                children: Self::no_children(builder),
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
                    type_: Self::integer(builder, 64),
                    children: Self::no_children(builder),
                    dictionary: Some(dictionary),
                }
            }
            Self::OffsetsCutInsideAnOffset => CraftedField {
                type_type: Type::Utf8,
                type_: Utf8::create(builder, &Utf8Args {}).as_union_value(),
                children: Self::no_children(builder),
                dictionary: None,
            },
            Self::FixedSizeListTooLongToCount => {
                let element_type = Self::integer(builder, 64);
                let element_children = Self::no_children(builder);
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
                type_: Self::integer(builder, 64),
                children: Self::no_children(builder),
                dictionary: None,
            },
        };
        let name = builder.create_string("value");
        Field::create(
            builder,
            &FieldArgs {
                name: Some(name),
                nullable: false,
                type_type: crafted.type_type,
                type_: Some(crafted.type_),
                dictionary: crafted.dictionary,
                children: Some(crafted.children),
                custom_metadata: None,
            },
        )
    }

    /// A signed integer type `bits` wide.
    fn integer(builder: &mut FlatBufferBuilder<'_>, bits: i32) -> WIPOffset<UnionWIPOffset> {
        Int::create(
            builder,
            &IntArgs {
                bitWidth: bits,
                is_signed: true,
            },
        )
        .as_union_value()
    }

    fn no_children<'a>(
        builder: &mut FlatBufferBuilder<'a>,
    ) -> WIPOffset<Vector<'a, ForwardsUOffset<Field<'a>>>> {
        builder.create_vector::<ForwardsUOffset<Field<'a>>>(&[])
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
                nodes: vec![FieldNode::new(1, 0)],
                buffers: vec![Buffer::new(0, 0), Buffer::new(0, 1 << 40)],
                variadic_buffer_counts: None,
                declared_body: 8,
                body: 8,
            },
            Self::ValidityShorterThanRows => CraftedBatch {
                nodes: vec![FieldNode::new(1, 1)],
                buffers: vec![Buffer::new(0, 0), Buffer::new(0, 8)],
                variadic_buffer_counts: None,
                declared_body: 8,
                body: 8,
            },
            // Nine bytes hold the two offsets one value needs and one byte more.
            Self::OffsetsCutInsideAnOffset => CraftedBatch {
                nodes: vec![FieldNode::new(1, 0)],
                buffers: vec![Buffer::new(0, 0), Buffer::new(0, 9), Buffer::new(16, 0)],
                variadic_buffer_counts: None,
                declared_body: 16,
                body: 16,
            },
            Self::VariadicBufferCounts => CraftedBatch {
                nodes: vec![FieldNode::new(1, 0)],
                buffers: vec![Buffer::new(0, 0), Buffer::new(0, 8)],
                variadic_buffer_counts: Some(vec![1]),
                declared_body: 8,
                body: 8,
            },
            // The list's length times its size of `i32::MAX` does not fit in 64 bits.
            Self::FixedSizeListTooLongToCount => CraftedBatch {
                nodes: vec![FieldNode::new(1 << 40, 0), FieldNode::new(0, 0)],
                buffers: vec![Buffer::new(0, 0), Buffer::new(0, 0), Buffer::new(0, 0)],
                variadic_buffer_counts: None,
                declared_body: 0,
                body: 0,
            },
            Self::BodyLongerThanStream => CraftedBatch {
                nodes: vec![FieldNode::new(1, 0)],
                buffers: vec![Buffer::new(0, 0), Buffer::new(0, 8)],
                variadic_buffer_counts: None,
                declared_body: 1 << 60,
                body: 8,
            },
        };
        Some(batch)
    }

    /// One record batch message of one row declaring what `batch` holds.
    fn batch_message(batch: &CraftedBatch) -> Vec<u8> {
        let mut builder = FlatBufferBuilder::new();
        let nodes = builder.create_vector(&batch.nodes);
        let buffers = builder.create_vector(&batch.buffers);
        let variadic_buffer_counts = batch
            .variadic_buffer_counts
            .as_ref()
            .map(|counts| builder.create_vector(counts));
        let record_batch = RecordBatch::create(
            &mut builder,
            &RecordBatchArgs {
                length: 1,
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
                bodyLength: batch.declared_body,
                custom_metadata: None,
            },
        );
        builder.finish(message, None);
        builder.finished_data().to_vec()
    }
}
