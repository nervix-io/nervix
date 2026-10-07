//! One Arrow IPC stream, checked before Arrow's reader reads any of it.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Reading a stream's messages one at a time without decoding their bodies, and
//!   everything a stream must hold before it reaches Arrow's reader: the continuation marker and
//!   lengths that frame each message, the schema message first with only the field types Nervix
//!   carries, record batch messages only and uncompressed, every record batch shaped as that
//!   schema says with its buffers inside its own body, and the stream's end. Opening Arrow's reader
//!   over a stream it accepted.
//! - **Depends on.** Arrow's IPC message definitions and its stream reader.
//! - **Must not know.** Who sent the stream, what its batches are for, or what happens to them.
//!
//! Arrow's stream reader trusts what a stream declares. It panics, rather than refusing the stream,
//! on a field type or a type parameter it does not implement, on a list without its one child, on
//! a buffer that reaches past its message body, on a validity bitmap shorter than the nulls it is
//! declared to hold, on an offsets buffer that ends inside an offset, on variadic buffer counts no
//! field takes and on a fixed-size list too long to count, and it allocates a message's metadata
//! and body from the lengths the stream declares before reading them. A stream from outside the
//! node is therefore scanned here first, and [`ScannedStream::reader`] is the only way the node
//! opens Arrow's reader over one.

use std::{io::Cursor, num::NonZeroUsize};

use arrow_ipc::{
    Buffer, Endianness, Field, Message, MessageHeader, Precision, RecordBatch, TimeUnit, Type,
    reader::StreamReader, root_as_message,
};
use arrow_schema::ArrowError;
use error_stack::Report;
use flatbuffers::Vector;
use meticulous::{OptionExt as _, ResultExt as _};
use thiserror::Error;

/// The marker that opens every message of a canonical Arrow IPC stream.
const CONTINUATION_MARKER: [u8; 4] = [0xff; 4];

/// The width of the continuation marker and of the metadata length that follows it.
const FRAME_WORD: usize = 4;

/// The width of one offset of a list, a text or a bytes field. Arrow reads an offsets buffer as
/// whole offsets after checking only that it holds enough of them.
const OFFSET_WIDTH: i64 = 4;

/// The longest field node a record batch may declare. Arrow's validation multiplies the length of a
/// fixed-size list by its size, itself at most `i32::MAX`, and panics when the product overflows; a
/// length within `i32` keeps that product inside 64 bits.
const MAX_FIELD_NODE_LENGTH: i32 = i32::MAX;

/// Why a stream is not one canonical Arrow IPC stream of the field types Nervix carries. Each
/// variant names the defect without the bytes, which may carry a payload.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(crate) enum IpcStreamError {
    #[error("the stream ends inside a message")]
    Truncated,
    #[error("a message does not open with the continuation marker")]
    ContinuationMarker,
    #[error("a message declares a negative metadata length")]
    NegativeMetadataLength,
    #[error("a message's metadata is not an Arrow IPC message: {reason}")]
    Metadata { reason: String },
    #[error("a message declares a body length outside this body")]
    BodyLength,
    #[error("bytes follow the end-of-stream marker")]
    TrailingBytes,
    #[error("the stream ends before its schema message")]
    MissingSchema,
    #[error("the stream does not open with its schema message")]
    SchemaNotFirst,
    #[error("the schema declares big-endian values")]
    BigEndian,
    #[error("the schema declares no field list")]
    MissingFields,
    /// `field` is the schema's field that declares the kind itself or nests the field that does.
    #[error("field {field} declares {kind}, which no Nervix type is carried as")]
    UnsupportedField {
        field: usize,
        kind: UnsupportedFieldKind,
    },
    #[error("the stream carries a {kind} message, which this stream never does")]
    UnexpectedMessage { kind: &'static str },
    #[error("a record batch is compressed")]
    Compressed,
    #[error("a record batch declares a negative row count")]
    NegativeRowCount,
    #[error("a record batch has {rows} rows, more than the {limit} one batch may carry")]
    TooManyRows { rows: u64, limit: usize },
    #[error("a record batch declares buffer {buffer} outside its message body")]
    BufferOutsideBody { buffer: usize },
    #[error("a record batch declares variadic buffer counts, which no Nervix type has")]
    VariadicBuffers,
    #[error(
        "a record batch declares {actual} field nodes, and its schema's fields take {expected}"
    )]
    NodeCount { expected: usize, actual: usize },
    #[error("a record batch declares {actual} buffers, and its schema's fields take {expected}")]
    BufferCount { expected: usize, actual: usize },
    #[error("a record batch declares field node {node} with a length or a null count no array has")]
    NodeLength { node: usize },
    #[error(
        "a record batch declares validity buffer {buffer} shorter than the nulls of its field need"
    )]
    ValidityTooShort { buffer: usize },
    #[error(
        "a record batch declares offsets buffer {buffer} of a length that is not a whole number \
         of offsets"
    )]
    OffsetsNotWhole { buffer: usize },
}

impl IpcStreamError {
    /// Whether the defect is a record batch at odds with the schema its own stream declares, as
    /// opposed to a stream that does not frame its messages or a schema Nervix does not carry.
    pub(crate) fn is_of_record_batch_shape(&self) -> bool {
        match self {
            Self::VariadicBuffers
            | Self::NodeCount { .. }
            | Self::BufferCount { .. }
            | Self::NodeLength { .. }
            | Self::ValidityTooShort { .. }
            | Self::OffsetsNotWhole { .. } => true,
            Self::Truncated
            | Self::ContinuationMarker
            | Self::NegativeMetadataLength
            | Self::Metadata { .. }
            | Self::BodyLength
            | Self::TrailingBytes
            | Self::MissingSchema
            | Self::SchemaNotFirst
            | Self::BigEndian
            | Self::MissingFields
            | Self::UnsupportedField { .. }
            | Self::UnexpectedMessage { .. }
            | Self::Compressed
            | Self::NegativeRowCount
            | Self::TooManyRows { .. }
            | Self::BufferOutsideBody { .. } => false,
        }
    }
}

/// What a field declares that no Nervix type is carried as. Nervix carries integers of 8, 16, 32
/// and 64 bits, floats of 32 and 64 bits, booleans, text, bytes, timestamps in nanoseconds, and
/// lists and fixed-size lists of those.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
pub enum UnsupportedFieldKind {
    #[strum(to_string = "the Arrow type {name}")]
    Type { name: &'static str },
    #[strum(to_string = "an integer of {bits} bits")]
    IntegerWidth { bits: i32 },
    #[strum(to_string = "a float of {precision} precision")]
    FloatPrecision { precision: &'static str },
    #[strum(to_string = "a timestamp in {unit} units")]
    TimestampUnit { unit: &'static str },
    #[strum(to_string = "a list of {children} child fields")]
    ListChildren { children: usize },
    #[strum(to_string = "a fixed-size list of {size} values")]
    FixedSizeListSize { size: i32 },
    #[strum(to_string = "a dictionary encoding")]
    Dictionary,
}

/// Where a stream may end.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StreamEnding {
    /// The end-of-stream marker ends the bytes, as a peer, a snapshot or a client producer writes
    /// a stream.
    Marker,
    /// At the end-of-stream marker or, as the Arrow format allows a writer that closes its stream
    /// instead, where the bytes end after a whole message. Bytes after the marker are left to the
    /// caller, which is told how many there are.
    MarkerOrEnd,
}

/// A stream the scan accepted, which alone opens Arrow's reader over it.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ScannedStream<'a> {
    /// The stream's bytes up to and including its end.
    stream: &'a [u8],
    /// How many record batch messages follow the schema message.
    pub(crate) record_batches: usize,
    /// How many bytes follow the stream's end. Only a stream that may end before its bytes do has
    /// any.
    pub(crate) trailing: usize,
}

impl<'a> ScannedStream<'a> {
    /// Arrow's reader over the scanned stream.
    pub(crate) fn reader(&self) -> Result<StreamReader<Cursor<&'a [u8]>>, ArrowError> {
        StreamReader::try_new(Cursor::new(self.stream), None)
    }
}

/// How the values of one field are laid out in a record batch after the validity bitmap every
/// field has.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FieldLayout {
    /// One buffer of values: a boolean, an integer, a float or a timestamp.
    Values,
    /// A buffer of offsets and a buffer of the bytes they delimit: text or bytes.
    Bytes,
    /// A buffer of offsets delimiting values of the field that follows: a list.
    List,
    /// No buffer of its own, and a fixed number of values of the field that follows: a fixed-size
    /// list.
    FixedSizeList,
}

impl FieldLayout {
    /// The layout a field's own type gives it, or what makes the type one Nervix does not carry.
    fn of(field: &Field<'_>) -> Result<Self, UnsupportedFieldKind> {
        match field.type_type() {
            Type::Int => {
                let int = field.type_as_int().verified(
                    "the message verifier admits a union type only beside its value, and this arm \
                     matched the integer type",
                );
                match int.bitWidth() {
                    8 | 16 | 32 | 64 => Ok(Self::Values),
                    bits => Err(UnsupportedFieldKind::IntegerWidth { bits }),
                }
            }
            Type::FloatingPoint => {
                let float = field.type_as_floating_point().verified(
                    "the message verifier admits a union type only beside its value, and this arm \
                     matched the floating point type",
                );
                match float.precision() {
                    Precision::SINGLE | Precision::DOUBLE => Ok(Self::Values),
                    other => {
                        let precision = other.variant_name().unwrap_or("unknown");
                        Err(UnsupportedFieldKind::FloatPrecision { precision })
                    }
                }
            }
            Type::Timestamp => {
                let timestamp = field.type_as_timestamp().verified(
                    "the message verifier admits a union type only beside its value, and this arm \
                     matched the timestamp type",
                );
                match timestamp.unit() {
                    TimeUnit::NANOSECOND => Ok(Self::Values),
                    other => {
                        let unit = other.variant_name().unwrap_or("unknown");
                        Err(UnsupportedFieldKind::TimestampUnit { unit })
                    }
                }
            }
            Type::Bool => Ok(Self::Values),
            Type::Utf8 | Type::Binary => Ok(Self::Bytes),
            Type::List => Ok(Self::List),
            Type::FixedSizeList => {
                let list = field.type_as_fixed_size_list().verified(
                    "the message verifier admits a union type only beside its value, and this arm \
                     matched the fixed-size list type",
                );
                let size = list.listSize();
                if size <= 0 {
                    return Err(UnsupportedFieldKind::FixedSizeListSize { size });
                }
                Ok(Self::FixedSizeList)
            }
            other => {
                let name = other.variant_name().unwrap_or("unknown");
                Err(UnsupportedFieldKind::Type { name })
            }
        }
    }

    /// Whether the field's values are values of one field it nests.
    fn nests_a_field(self) -> bool {
        match self {
            Self::List | Self::FixedSizeList => true,
            Self::Values | Self::Bytes => false,
        }
    }

    /// The buffers a field of this layout declares, in the order a record batch lists them.
    fn buffers(self) -> &'static [BufferRole] {
        match self {
            Self::Values => &[BufferRole::Validity, BufferRole::Unread],
            Self::Bytes => &[
                BufferRole::Validity,
                BufferRole::Offsets,
                BufferRole::Unread,
            ],
            Self::List => &[BufferRole::Validity, BufferRole::Offsets],
            Self::FixedSizeList => &[BufferRole::Validity],
        }
    }
}

/// What one buffer of a field holds, which decides what the scan checks of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BufferRole {
    /// The validity bitmap: one bit for each value, read only when the field counts a null.
    Validity,
    /// The offsets of a list, a text or a bytes field, which Arrow reads as whole offsets.
    Offsets,
    /// Values or bytes, whose length Arrow checks itself before it reads them.
    Unread,
}

/// What a scanned schema makes every record batch of its stream declare.
#[derive(Debug)]
struct StreamShape {
    /// The layout of each field in the order a record batch lists its field nodes: a field, then
    /// the field it nests.
    fields: Vec<FieldLayout>,
    /// How many buffers those fields take together.
    buffers: usize,
}

impl StreamShape {
    /// The shape the schema message declares, or why it is not a schema of Nervix's field types.
    fn of_schema(message: &Message<'_>) -> Result<Self, Report<IpcStreamError>> {
        let schema = message.header_as_schema().verified(
            "the message verifier admits a union type only beside its value, and the caller \
             checked the type is a schema",
        );
        if schema.endianness() != Endianness::Little {
            return Err(Report::new(IpcStreamError::BigEndian));
        }
        let Some(schema_fields) = schema.fields() else {
            return Err(Report::new(IpcStreamError::MissingFields));
        };
        let mut fields = Vec::new();
        for (index, field) in schema_fields.iter().enumerate() {
            Self::push_field(&mut fields, field).map_err(|kind| {
                Report::new(IpcStreamError::UnsupportedField { field: index, kind })
            })?;
        }
        let mut buffers = 0_usize;
        for layout in &fields {
            buffers = buffers
                .checked_add(layout.buffers().len())
                .assured("every counted field occupies bytes of a message that fits in memory");
        }
        Ok(Self { fields, buffers })
    }

    /// Appends the layout of `field` and of every field it nests, one inside the other. The
    /// message verifier bounds how deep tables nest, which bounds this walk.
    fn push_field(
        fields: &mut Vec<FieldLayout>,
        field: Field<'_>,
    ) -> Result<(), UnsupportedFieldKind> {
        let mut field = field;
        loop {
            if field.dictionary().is_some() {
                return Err(UnsupportedFieldKind::Dictionary);
            }
            let layout = FieldLayout::of(&field)?;
            fields.push(layout);
            if !layout.nests_a_field() {
                return Ok(());
            }
            let children = field.children().unwrap_or_default();
            if children.len() != 1 {
                return Err(UnsupportedFieldKind::ListChildren {
                    children: children.len(),
                });
            }
            field = children.get(0);
        }
    }

    /// Checks one record batch message: uncompressed, a row count within `max_rows`, every buffer
    /// it declares inside the body the message declares, and the field nodes and buffers this
    /// shape's fields take, each holding what Arrow's reader reads without checking.
    fn check_record_batch(
        &self,
        message: &Message<'_>,
        max_rows: Option<NonZeroUsize>,
    ) -> Result<(), Report<IpcStreamError>> {
        let batch = message.header_as_record_batch().verified(
            "the message verifier admits a union type only beside its value, and the caller \
             checked the type is a record batch",
        );
        if batch.compression().is_some() {
            return Err(Report::new(IpcStreamError::Compressed));
        }
        let Ok(rows) = u64::try_from(batch.length()) else {
            return Err(Report::new(IpcStreamError::NegativeRowCount));
        };
        if let Some(limit) = max_rows {
            let within = match usize::try_from(rows) {
                Ok(rows) => rows <= limit.get(),
                Err(_) => false,
            };
            if !within {
                return Err(Report::new(IpcStreamError::TooManyRows {
                    rows,
                    limit: limit.get(),
                }));
            }
        }
        let body_length = message.bodyLength();
        let buffers = batch.buffers().unwrap_or_default();
        for (index, buffer) in buffers.iter().enumerate() {
            let end = buffer.offset().checked_add(buffer.length());
            let inside = buffer.offset() >= 0
                && buffer.length() >= 0
                && matches!(end, Some(end) if end <= body_length);
            if !inside {
                return Err(Report::new(IpcStreamError::BufferOutsideBody {
                    buffer: index,
                }));
            }
        }
        self.check_fields(&batch, buffers)
    }

    /// Checks that the record batch declares exactly the field nodes and buffers this shape's
    /// fields take, and that each node and the buffers Arrow's reader reads by it agree.
    fn check_fields(
        &self,
        batch: &RecordBatch<'_>,
        buffers: Vector<'_, Buffer>,
    ) -> Result<(), Report<IpcStreamError>> {
        let variadic_counts = batch.variadicBufferCounts().unwrap_or_default();
        if !variadic_counts.is_empty() {
            return Err(Report::new(IpcStreamError::VariadicBuffers));
        }
        let nodes = batch.nodes().unwrap_or_default();
        if nodes.len() != self.fields.len() {
            return Err(Report::new(IpcStreamError::NodeCount {
                expected: self.fields.len(),
                actual: nodes.len(),
            }));
        }
        let mut buffers = DeclaredBuffers {
            buffers,
            next: 0,
            expected: self.buffers,
        };
        for (index, (layout, node)) in self.fields.iter().zip(nodes.iter()).enumerate() {
            let length = node.length();
            let nulls = node.null_count();
            let countable =
                nulls >= 0 && nulls <= length && length <= i64::from(MAX_FIELD_NODE_LENGTH);
            if !countable {
                return Err(Report::new(IpcStreamError::NodeLength { node: index }));
            }
            let bitmap = length
                .checked_add(7)
                .assured("a length within i32 leaves room in 64 bits")
                / 8;
            for role in layout.buffers() {
                let buffer = buffers.take()?;
                match role {
                    BufferRole::Validity => {
                        if nulls > 0 && buffer.length < bitmap {
                            return Err(Report::new(IpcStreamError::ValidityTooShort {
                                buffer: buffer.index,
                            }));
                        }
                    }
                    BufferRole::Offsets => {
                        if buffer.length % OFFSET_WIDTH != 0 {
                            return Err(Report::new(IpcStreamError::OffsetsNotWhole {
                                buffer: buffer.index,
                            }));
                        }
                    }
                    BufferRole::Unread => {}
                }
            }
        }
        buffers.finish()
    }
}

/// The buffers one record batch declares, taken in the order its schema's fields use them.
struct DeclaredBuffers<'a> {
    buffers: Vector<'a, Buffer>,
    /// The place of the next buffer to take.
    next: usize,
    /// How many buffers the schema's fields take.
    expected: usize,
}

/// One buffer a record batch declares: its place among the record batch's buffers and its length
/// in bytes, which the caller has checked lies inside the message body.
struct DeclaredBuffer {
    index: usize,
    length: i64,
}

impl DeclaredBuffers<'_> {
    /// The next buffer, which a record batch that declares too few does not have.
    fn take(&mut self) -> Result<DeclaredBuffer, Report<IpcStreamError>> {
        let index = self.next;
        if index >= self.buffers.len() {
            return Err(self.miscounted());
        }
        self.next = index
            .checked_add(1)
            .verified("an index below a vector's length has a successor");
        Ok(DeclaredBuffer {
            index,
            length: self.buffers.get(index).length(),
        })
    }

    /// Checks that the fields took every buffer the record batch declares.
    fn finish(self) -> Result<(), Report<IpcStreamError>> {
        if self.next != self.buffers.len() {
            return Err(self.miscounted());
        }
        Ok(())
    }

    fn miscounted(&self) -> Report<IpcStreamError> {
        Report::new(IpcStreamError::BufferCount {
            expected: self.expected,
            actual: self.buffers.len(),
        })
    }
}

/// The messages of an Arrow IPC stream, read one at a time without decoding their bodies.
pub(crate) struct IpcStream<'a> {
    body: &'a [u8],
    offset: usize,
    ending: StreamEnding,
}

impl<'a> IpcStream<'a> {
    /// A stream that the end-of-stream marker must end.
    pub(crate) fn new(body: &'a [u8]) -> Self {
        Self::ending(body, StreamEnding::Marker)
    }

    /// A stream that may end as `ending` allows.
    pub(crate) fn ending(body: &'a [u8], ending: StreamEnding) -> Self {
        Self {
            body,
            offset: 0,
            ending,
        }
    }

    /// Checks that the stream is one schema message of the field types Nervix carries,
    /// uncompressed record batch messages of at most `max_rows` rows shaped as that schema says
    /// with their buffers inside their bodies, and the stream's end.
    pub(crate) fn scan(
        mut self,
        max_rows: Option<NonZeroUsize>,
    ) -> Result<ScannedStream<'a>, Report<IpcStreamError>> {
        let mut shape: Option<StreamShape> = None;
        let mut record_batches = 0_usize;
        while let Some(message) = self.next_message()? {
            let header = message.header_type();
            let Some(declared) = &shape else {
                if header != MessageHeader::Schema {
                    return Err(Report::new(IpcStreamError::SchemaNotFirst));
                }
                shape = Some(StreamShape::of_schema(&message)?);
                continue;
            };
            if header != MessageHeader::RecordBatch {
                let kind = header.variant_name().unwrap_or("unknown");
                return Err(Report::new(IpcStreamError::UnexpectedMessage { kind }));
            }
            declared.check_record_batch(&message, max_rows)?;
            record_batches = record_batches
                .checked_add(1)
                .assured("every counted message occupies bytes of a body that fits in memory");
        }
        if shape.is_none() {
            return Err(Report::new(IpcStreamError::MissingSchema));
        }
        let body: &'a [u8] = self.body;
        let (stream, after) = body
            .split_at_checked(self.offset)
            .verified("the scan only moves its offset over bytes it took from the body");
        Ok(ScannedStream {
            stream,
            record_batches,
            trailing: after.len(),
        })
    }

    /// The next message's header, or `None` at the stream's end.
    fn next_message(&mut self) -> Result<Option<Message<'a>>, Report<IpcStreamError>> {
        if self.ending == StreamEnding::MarkerOrEnd && self.offset == self.body.len() {
            return Ok(None);
        }
        let marker = self.take(FRAME_WORD)?;
        if marker != CONTINUATION_MARKER {
            return Err(Report::new(IpcStreamError::ContinuationMarker));
        }
        let length_bytes = self.take(FRAME_WORD)?;
        let length = i32::from_le_bytes(
            length_bytes
                .try_into()
                .verified("take returned exactly the four bytes it was asked for"),
        );
        if length == 0 {
            if self.ending == StreamEnding::Marker && self.offset != self.body.len() {
                return Err(Report::new(IpcStreamError::TrailingBytes));
            }
            return Ok(None);
        }
        let Ok(length) = usize::try_from(length) else {
            return Err(Report::new(IpcStreamError::NegativeMetadataLength));
        };
        let metadata = self.take(length)?;
        let message = root_as_message(metadata).map_err(|error| {
            Report::new(IpcStreamError::Metadata {
                reason: error.to_string(),
            })
        })?;
        let Ok(body_length) = usize::try_from(message.bodyLength()) else {
            return Err(Report::new(IpcStreamError::BodyLength));
        };
        self.take(body_length)?;
        Ok(Some(message))
    }

    /// The next `length` bytes of the body.
    fn take(&mut self, length: usize) -> Result<&'a [u8], Report<IpcStreamError>> {
        let body: &'a [u8] = self.body;
        let Some(end) = self.offset.checked_add(length) else {
            return Err(Report::new(IpcStreamError::Truncated));
        };
        let Some(bytes) = body.get(self.offset..end) else {
            return Err(Report::new(IpcStreamError::Truncated));
        };
        self.offset = end;
        Ok(bytes)
    }
}
