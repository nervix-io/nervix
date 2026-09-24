//! Typed extraction of values from JSON documents held in `STRING` columns.
//!
//! One scan parses each document of a column once, with simd-json's SIMD structural indexing, and
//! answers every extraction a program makes from that column from the same parse. The parse is a
//! tape of the document's nodes; each extraction follows its path over the tape and appends the
//! value it finds directly to a typed Arrow column of its declared type. No document is ever held
//! as an untyped value, and no row is ever held as a map of fields.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The declared result types a JSON extraction reads, the document limits, the scan
//!   over a column of documents, the path walk over a parsed tape, the conversion of JSON values to
//!   typed columns, and the defects each of those steps can find.
//! - **Depends on.** simd-json's tape, Arrow arrays and buffers, and the `JsonPath` Model.
//! - **Must not know.** Registers, instructions, arms, or how a program is compiled.

use std::{fmt, num::NonZeroU32};

use arrow_array::{
    Array, ArrayRef, BooleanArray, FixedSizeListArray, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, ListArray, StringArray, StructArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow_buffer::{BooleanBufferBuilder, Buffer, NullBufferBuilder, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Fields};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_approx_into::{ApproxInto as _, CheckedApproxInto as _};
use nervix_models::{JsonPath, JsonPathStep, ParseAsType};
use simd_json::{Buffers, Node, StaticNode};
use thiserror::Error;
use triomphe::Arc;

use crate::{
    error::{ErrorCode, RowErrors, SideError, SideErrorReason},
    program::{CastFailure, Span},
};

/// The longest document an extraction reads, in bytes. A longer one is a defect of that document.
pub const MAX_DOCUMENT_BYTES: usize = 16 * 1024 * 1024;

/// The deepest an extraction's document may nest objects and arrays. A document nested deeper is
/// a defect of that document.
pub const MAX_DOCUMENT_DEPTH: usize = 128;

/// The type a `JSON_VALUE` declares for the value it reads.
///
/// JSON has no date or byte type, so neither `DATETIME` nor `BYTES` can be read: text holding one
/// is read as a `STRING` and converted explicitly.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum JsonTarget {
    Bool,
    String,
    UInt8,
    Int8,
    UInt16,
    Int16,
    UInt32,
    Int32,
    UInt64,
    Int64,
    Float32,
    Float64,
    /// A JSON array of any length, read as a `VEC` of its elements.
    Vec(Arc<JsonTarget>),
    /// A JSON array of exactly `len` elements, read as a fixed `ARRAY`.
    Array {
        element: Arc<JsonTarget>,
        len: NonZeroU32,
    },
}

/// Why a declared type cannot be the result of a JSON extraction.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum JsonTargetDefect {
    /// A type no JSON value holds.
    #[error("{0}, which no JSON value is; read the text as STRING and convert it explicitly")]
    NoJsonValue(ParseAsType),
    /// A fixed `ARRAY` wider than an Arrow fixed-size list holds.
    #[error("an ARRAY of length {0}, which exceeds 2147483647")]
    ArrayTooWide(NonZeroU32),
}

impl TryFrom<&ParseAsType> for JsonTarget {
    type Error = JsonTargetDefect;

    fn try_from(declared: &ParseAsType) -> Result<Self, Self::Error> {
        let target = match declared {
            ParseAsType::Bool => Self::Bool,
            ParseAsType::String => Self::String,
            ParseAsType::U8 => Self::UInt8,
            ParseAsType::I8 => Self::Int8,
            ParseAsType::U16 => Self::UInt16,
            ParseAsType::I16 => Self::Int16,
            ParseAsType::U32 => Self::UInt32,
            ParseAsType::I32 => Self::Int32,
            ParseAsType::U64 => Self::UInt64,
            ParseAsType::I64 => Self::Int64,
            ParseAsType::F32 => Self::Float32,
            ParseAsType::F64 => Self::Float64,
            ParseAsType::Vec { element } => Self::Vec(Arc::new(Self::try_from(element.as_ref())?)),
            ParseAsType::Array { element, len } => {
                if i32::try_from(len.get()).is_err() {
                    return Err(JsonTargetDefect::ArrayTooWide(*len));
                }
                Self::Array {
                    element: Arc::new(Self::try_from(element.as_ref())?),
                    len: *len,
                }
            }
            ParseAsType::Datetime | ParseAsType::Bytes => {
                return Err(JsonTargetDefect::NoJsonValue(declared.clone()));
            }
        };
        Ok(target)
    }
}

impl JsonTarget {
    /// The Arrow type of the column this target reads into, which is the type a schema field of
    /// the same declared type has: collection elements are never null.
    pub fn data_type(&self) -> DataType {
        match self {
            Self::Bool => DataType::Boolean,
            Self::String => DataType::Utf8,
            Self::UInt8 => DataType::UInt8,
            Self::Int8 => DataType::Int8,
            Self::UInt16 => DataType::UInt16,
            Self::Int16 => DataType::Int16,
            Self::UInt32 => DataType::UInt32,
            Self::Int32 => DataType::Int32,
            Self::UInt64 => DataType::UInt64,
            Self::Int64 => DataType::Int64,
            Self::Float32 => DataType::Float32,
            Self::Float64 => DataType::Float64,
            Self::Vec(element) => DataType::List(element.element_field()),
            Self::Array { element, len } => {
                DataType::FixedSizeList(element.element_field(), Self::fixed_width(*len))
            }
        }
    }

    /// The field of a collection whose elements have this type.
    fn element_field(&self) -> arrow_schema::FieldRef {
        std::sync::Arc::new(Field::new("item", self.data_type(), false))
    }

    fn fixed_width(len: NonZeroU32) -> i32 {
        i32::try_from(len.get())
            .verified("a JSON target is built only from an ARRAY length that fits an i32")
    }
}

impl fmt::Display for JsonTarget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bool => formatter.write_str("BOOL"),
            Self::String => formatter.write_str("STRING"),
            Self::UInt8 => formatter.write_str("U8"),
            Self::Int8 => formatter.write_str("I8"),
            Self::UInt16 => formatter.write_str("U16"),
            Self::Int16 => formatter.write_str("I16"),
            Self::UInt32 => formatter.write_str("U32"),
            Self::Int32 => formatter.write_str("I32"),
            Self::UInt64 => formatter.write_str("U64"),
            Self::Int64 => formatter.write_str("I64"),
            Self::Float32 => formatter.write_str("F32"),
            Self::Float64 => formatter.write_str("F64"),
            Self::Vec(element) => write!(formatter, "VEC<{element}>"),
            Self::Array { element, len } => write!(formatter, "ARRAY<{element}, {len}>"),
        }
    }
}

/// The operation an extraction is written as, which its errors name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, strum::Display)]
pub enum JsonOperation {
    #[strum(to_string = "JSON_VALUE")]
    JsonValue,
    #[strum(to_string = "TRY_JSON_VALUE")]
    TryJsonValue,
    #[strum(to_string = "JSON_EXISTS")]
    JsonExists,
}

/// What an extraction answers for the value its path leads to.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum JsonOutput {
    /// The value, read as `target`. A missing value and JSON null are a typed null. A document or
    /// value that cannot be read as `target` fails the row or yields a typed null, as `on_failure`
    /// says.
    Value {
        target: Arc<JsonTarget>,
        on_failure: CastFailure,
    },
    /// Whether the path leads to a value at all, JSON null included. A document that cannot be
    /// read fails the row.
    Exists,
}

impl JsonOutput {
    pub fn data_type(&self) -> DataType {
        match self {
            Self::Value { target, .. } => target.data_type(),
            Self::Exists => DataType::Boolean,
        }
    }

    pub const fn operation(&self) -> JsonOperation {
        match self {
            Self::Value {
                on_failure: CastFailure::Error,
                ..
            } => JsonOperation::JsonValue,
            Self::Value {
                on_failure: CastFailure::Null,
                ..
            } => JsonOperation::TryJsonValue,
            Self::Exists => JsonOperation::JsonExists,
        }
    }

    /// Whether a document or value this output cannot read fails the row, rather than yielding a
    /// typed null.
    pub const fn reports_defects(&self) -> bool {
        match self {
            Self::Value { on_failure, .. } => on_failure.reports_error(),
            Self::Exists => true,
        }
    }
}

/// One extraction: the path it follows and what it answers there.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct JsonExtraction {
    pub path: Arc<JsonPath>,
    pub output: JsonOutput,
}

/// One extraction a scan answers, with the span of the operation whose failures it reports.
#[derive(Debug, Clone, PartialEq)]
pub struct JsonScanOutput {
    pub extraction: JsonExtraction,
    pub span: Span,
}

/// The kind of JSON value an extraction found where its declared type holds another.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
pub enum JsonKind {
    #[strum(to_string = "JSON null")]
    Null,
    #[strum(to_string = "a JSON boolean")]
    Boolean,
    #[strum(to_string = "a JSON number")]
    Number,
    /// A number with a fractional part, read where an integer type is declared.
    #[strum(to_string = "a fractional JSON number")]
    FractionalNumber,
    #[strum(to_string = "a JSON string")]
    String,
    #[strum(to_string = "a JSON array")]
    Array,
    #[strum(to_string = "a JSON object")]
    Object,
}

/// Where a defect lies relative to the value a path leads to: the value itself, or an element
/// nested inside it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
pub enum JsonPlace {
    #[strum(to_string = "at")]
    Value,
    #[strum(to_string = "in")]
    Element,
}

impl JsonPlace {
    /// What the declared type expects, phrased for this place.
    fn expectation(self, expected: &JsonTarget) -> String {
        match self {
            Self::Value => format!("{expected} is declared"),
            Self::Element => format!("{expected} elements are declared"),
        }
    }
}

/// Why one row's extraction failed.
#[derive(Debug, Clone, PartialEq, Error)]
pub enum JsonDefect {
    #[error("{0} document is not valid JSON")]
    Malformed(JsonOperation),
    #[error("{0} document exceeds {MAX_DOCUMENT_BYTES} bytes")]
    DocumentTooLarge(JsonOperation),
    #[error("{0} document nests deeper than {MAX_DOCUMENT_DEPTH} levels")]
    DocumentTooDeep(JsonOperation),
    #[error(
        "JSON_VALUE found {found} {place} {path} where {expectation}",
        expectation = place.expectation(expected)
    )]
    TypeMismatch {
        path: Arc<JsonPath>,
        place: JsonPlace,
        found: JsonKind,
        expected: Arc<JsonTarget>,
    },
    #[error("JSON_VALUE number {place} {path} does not fit {target}")]
    OutOfRange {
        path: Arc<JsonPath>,
        place: JsonPlace,
        target: Arc<JsonTarget>,
    },
    #[error("JSON_VALUE array {place} {path} has {found} elements where {declared} are declared")]
    ArrayLength {
        path: Arc<JsonPath>,
        place: JsonPlace,
        found: usize,
        declared: NonZeroU32,
    },
    /// A result whose text or elements do not fit one Arrow column of its type.
    #[error("{operation} result exceeds what one {target} column holds")]
    ResultTooLarge {
        operation: JsonOperation,
        target: Arc<JsonTarget>,
    },
}

impl JsonDefect {
    /// The code a message failed by this defect reports: a document or value that does not read
    /// as its declared type is a failed conversion, a document beyond the limits is an invalid
    /// argument, and a result too large for its column is an overflow.
    pub const fn code(&self) -> ErrorCode {
        match self {
            Self::Malformed(_)
            | Self::TypeMismatch { .. }
            | Self::OutOfRange { .. }
            | Self::ArrayLength { .. } => ErrorCode::CastFailed,
            Self::DocumentTooLarge(_) | Self::DocumentTooDeep(_) => ErrorCode::InvalidArgument,
            Self::ResultTooLarge { .. } => ErrorCode::Overflow,
        }
    }

    /// Whether this defect belongs to the document or value being read, which a tolerant
    /// extraction answers with a typed null, rather than to the column being built.
    const fn concerns_the_document(&self) -> bool {
        match self {
            Self::Malformed(_)
            | Self::DocumentTooLarge(_)
            | Self::DocumentTooDeep(_)
            | Self::TypeMismatch { .. }
            | Self::OutOfRange { .. }
            | Self::ArrayLength { .. } => true,
            Self::ResultTooLarge { .. } => false,
        }
    }
}

/// A defect of one value, found while converting it and before the path is attached.
enum ValueDefect {
    TypeMismatch {
        place: JsonPlace,
        found: JsonKind,
        expected: Arc<JsonTarget>,
    },
    OutOfRange {
        place: JsonPlace,
        target: Arc<JsonTarget>,
    },
    ArrayLength {
        place: JsonPlace,
        found: usize,
        declared: NonZeroU32,
    },
    ResultTooLarge,
}

impl ValueDefect {
    fn into_defect(self, extraction: &JsonExtraction, target: &Arc<JsonTarget>) -> JsonDefect {
        let path = extraction.path.clone();
        match self {
            Self::TypeMismatch {
                place,
                found,
                expected,
            } => JsonDefect::TypeMismatch {
                path,
                place,
                found,
                expected,
            },
            Self::OutOfRange { place, target } => JsonDefect::OutOfRange {
                path,
                place,
                target,
            },
            Self::ArrayLength {
                place,
                found,
                declared,
            } => JsonDefect::ArrayLength {
                path,
                place,
                found,
                declared,
            },
            Self::ResultTooLarge => JsonDefect::ResultTooLarge {
                operation: extraction.output.operation(),
                target: target.clone(),
            },
        }
    }
}

/// The number of tape nodes the value starting at `node` spans, itself included.
const fn node_span(node: &Node<'_>) -> usize {
    match node {
        Node::Object { count, .. } | Node::Array { count, .. } => *count + 1,
        Node::String(_) | Node::Static(_) => 1,
    }
}

/// The kind of the JSON value `node` starts.
fn node_kind(node: &Node<'_>) -> JsonKind {
    match node {
        Node::String(_) => JsonKind::String,
        Node::Object { .. } => JsonKind::Object,
        Node::Array { .. } => JsonKind::Array,
        Node::Static(StaticNode::Null) => JsonKind::Null,
        Node::Static(StaticNode::Bool(_)) => JsonKind::Boolean,
        Node::Static(StaticNode::F64(value)) if value.fract() != 0.0 => JsonKind::FractionalNumber,
        Node::Static(StaticNode::I64(_) | StaticNode::U64(_) | StaticNode::F64(_)) => {
            JsonKind::Number
        }
    }
}

/// A parsed document: the nodes of its tape, in document order.
struct Document<'tape, 'input> {
    tape: &'tape [Node<'input>],
}

impl<'input> Document<'_, 'input> {
    fn node(&self, index: usize) -> &Node<'input> {
        self.tape
            .get(index)
            .verified("the tape index is the root or was reached by stepping over whole values")
    }

    /// The tape index of the value `path` leads to, or `None` when no value is there: a member
    /// the object lacks, an index past the end of the array, or a step into a value that is not
    /// an object or an array. Of several members with one name, the last is the one read.
    fn locate(&self, path: &JsonPath) -> Option<usize> {
        let mut index = 0;
        for step in path.steps() {
            let next = match (step, self.node(index)) {
                (JsonPathStep::Member(name), Node::Object { len, .. }) => {
                    self.member(index, *len, name)
                }
                (JsonPathStep::Element(position), Node::Array { len, .. }) => {
                    self.element(index, *len, *position)
                }
                (JsonPathStep::Member(_) | JsonPathStep::Element(_), _) => None,
            };
            index = next?;
        }
        Some(index)
    }

    fn member(&self, object: usize, len: usize, name: &str) -> Option<usize> {
        let mut found = None;
        let mut key = object + 1;
        for _ in 0..len {
            let value = key + 1;
            if let Node::String(member) = self.node(key)
                && *member == name
            {
                found = Some(value);
            }
            key = value + node_span(self.node(value));
        }
        found
    }

    fn element(&self, array: usize, len: usize, position: u32) -> Option<usize> {
        let position = usize::try_from(position).ok()?;
        if position >= len {
            return None;
        }
        let mut element = array + 1;
        for _ in 0..position {
            element += node_span(self.node(element));
        }
        Some(element)
    }

    /// Whether objects and arrays nest deeper than `limit` anywhere in the document. `open_ends`
    /// is scratch space for the tape index at which each open container ends.
    fn nests_deeper_than(&self, limit: usize, open_ends: &mut Vec<usize>) -> bool {
        open_ends.clear();
        for (index, node) in self.tape.iter().enumerate() {
            while let Some(end) = open_ends.last()
                && *end <= index
            {
                open_ends.pop();
            }
            if let Node::Object { count, .. } | Node::Array { count, .. } = node {
                open_ends.push(index + count + 1);
                if open_ends.len() > limit {
                    return true;
                }
            }
        }
        false
    }
}

/// The values of one column being built, with no validity of their own: a top-level column keeps
/// its validity beside them, and collection elements are never null.
///
/// Every variant can be cut back to a length it had before, so a value that fails part way
/// through a collection leaves nothing of itself behind.
enum ValueColumn {
    Bool(BooleanBufferBuilder),
    String {
        offsets: Vec<i32>,
        bytes: Vec<u8>,
    },
    UInt8(Vec<u8>),
    Int8(Vec<i8>),
    UInt16(Vec<u16>),
    Int16(Vec<i16>),
    UInt32(Vec<u32>),
    Int32(Vec<i32>),
    UInt64(Vec<u64>),
    Int64(Vec<i64>),
    Float32(Vec<f32>),
    Float64(Vec<f64>),
    /// A `VEC` of `element`.
    Vec {
        element: Arc<JsonTarget>,
        offsets: Vec<i32>,
        elements: Box<ValueColumn>,
    },
    /// An `ARRAY` of `len` elements of `element`, `width` being `len` as a count.
    Array {
        element: Arc<JsonTarget>,
        len: NonZeroU32,
        width: usize,
        elements: Box<ValueColumn>,
    },
}

/// Reads an integer value of the target type from a number node.
macro_rules! integer_value {
    ($node:expr, $place:expr, $target:expr, $integer:ty) => {
        match $node {
            Node::Static(StaticNode::I64(value)) => <$integer>::try_from(*value).ok(),
            Node::Static(StaticNode::U64(value)) => <$integer>::try_from(*value).ok(),
            Node::Static(StaticNode::F64(value)) if value.fract() == 0.0 => {
                (*value).checked_approx_into::<$integer>()
            }
            node => {
                return Err(ValueDefect::TypeMismatch {
                    place: $place,
                    found: node_kind(node),
                    expected: $target.clone(),
                });
            }
        }
        .ok_or_else(|| ValueDefect::OutOfRange {
            place: $place,
            target: $target.clone(),
        })?
    };
}

impl ValueColumn {
    fn new(target: &JsonTarget) -> Self {
        match target {
            JsonTarget::Bool => Self::Bool(BooleanBufferBuilder::new(0)),
            JsonTarget::String => Self::String {
                offsets: vec![0],
                bytes: Vec::new(),
            },
            JsonTarget::UInt8 => Self::UInt8(Vec::new()),
            JsonTarget::Int8 => Self::Int8(Vec::new()),
            JsonTarget::UInt16 => Self::UInt16(Vec::new()),
            JsonTarget::Int16 => Self::Int16(Vec::new()),
            JsonTarget::UInt32 => Self::UInt32(Vec::new()),
            JsonTarget::Int32 => Self::Int32(Vec::new()),
            JsonTarget::UInt64 => Self::UInt64(Vec::new()),
            JsonTarget::Int64 => Self::Int64(Vec::new()),
            JsonTarget::Float32 => Self::Float32(Vec::new()),
            JsonTarget::Float64 => Self::Float64(Vec::new()),
            JsonTarget::Vec(element) => Self::Vec {
                element: element.clone(),
                offsets: vec![0],
                elements: Box::new(Self::new(element)),
            },
            JsonTarget::Array { element, len } => Self::Array {
                element: element.clone(),
                len: *len,
                width: usize::try_from(len.get())
                    .assured("every supported target platform addresses at least 32 bits"),
                elements: Box::new(Self::new(element)),
            },
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Bool(values) => values.len(),
            Self::String { offsets, .. } | Self::Vec { offsets, .. } => offsets.len() - 1,
            Self::UInt8(values) => values.len(),
            Self::Int8(values) => values.len(),
            Self::UInt16(values) => values.len(),
            Self::Int16(values) => values.len(),
            Self::UInt32(values) => values.len(),
            Self::Int32(values) => values.len(),
            Self::UInt64(values) => values.len(),
            Self::Int64(values) => values.len(),
            Self::Float32(values) => values.len(),
            Self::Float64(values) => values.len(),
            Self::Array {
                width, elements, ..
            } => elements.len() / *width,
        }
    }

    /// Cuts the column back to its first `len` values.
    fn truncate(&mut self, len: usize) {
        match self {
            Self::Bool(values) => values.truncate(len),
            Self::String { offsets, bytes } => {
                offsets.truncate(len + 1);
                bytes.truncate(Self::offset_position(offsets, len));
            }
            Self::UInt8(values) => values.truncate(len),
            Self::Int8(values) => values.truncate(len),
            Self::UInt16(values) => values.truncate(len),
            Self::Int16(values) => values.truncate(len),
            Self::UInt32(values) => values.truncate(len),
            Self::Int32(values) => values.truncate(len),
            Self::UInt64(values) => values.truncate(len),
            Self::Int64(values) => values.truncate(len),
            Self::Float32(values) => values.truncate(len),
            Self::Float64(values) => values.truncate(len),
            Self::Vec {
                offsets, elements, ..
            } => {
                offsets.truncate(len + 1);
                elements.truncate(Self::offset_position(offsets, len));
            }
            Self::Array {
                width, elements, ..
            } => {
                let kept = len
                    .checked_mul(*width)
                    .verified("the column already holds this many elements, so the count fits");
                elements.truncate(kept);
            }
        }
    }

    /// The position the offset of value `index` records.
    fn offset_position(offsets: &[i32], index: usize) -> usize {
        let offset = offsets
            .get(index)
            .verified("a column keeps one offset more than it has values");
        usize::try_from(*offset).verified("a column writes only offsets it counted up from zero")
    }

    /// The offset that ends a value whose content reaches `position`.
    fn next_offset(position: usize) -> Result<i32, ValueDefect> {
        i32::try_from(position).map_err(|_| ValueDefect::ResultTooLarge)
    }

    /// Appends the value that stands under a null: nothing for a variable-length value, and a
    /// zero, `false`, or a fixed array of them otherwise.
    fn push_placeholder(&mut self) {
        match self {
            Self::Bool(values) => values.append(false),
            Self::String { offsets, .. } | Self::Vec { offsets, .. } => {
                let last = *offsets.last().verified("a column starts with offset zero");
                offsets.push(last);
            }
            Self::UInt8(values) => values.push(0),
            Self::Int8(values) => values.push(0),
            Self::UInt16(values) => values.push(0),
            Self::Int16(values) => values.push(0),
            Self::UInt32(values) => values.push(0),
            Self::Int32(values) => values.push(0),
            Self::UInt64(values) => values.push(0),
            Self::Int64(values) => values.push(0),
            Self::Float32(values) => values.push(0.0),
            Self::Float64(values) => values.push(0.0),
            Self::Array {
                width, elements, ..
            } => {
                for _ in 0..*width {
                    elements.push_placeholder();
                }
            }
        }
    }

    /// Appends the value starting at tape index `index`, read as `target`. On a defect the column
    /// may hold part of the value, which the caller cuts back.
    fn append(
        &mut self,
        document: &Document<'_, '_>,
        index: usize,
        target: &Arc<JsonTarget>,
        place: JsonPlace,
    ) -> Result<(), ValueDefect> {
        let node = document.node(index);
        match self {
            Self::Bool(values) => {
                let Node::Static(StaticNode::Bool(value)) = node else {
                    return Err(Self::mismatch(node, target, place));
                };
                values.append(*value);
            }
            Self::String { offsets, bytes } => {
                let Node::String(value) = node else {
                    return Err(Self::mismatch(node, target, place));
                };
                bytes.extend_from_slice(value.as_bytes());
                offsets.push(Self::next_offset(bytes.len())?);
            }
            Self::UInt8(values) => values.push(integer_value!(node, place, target, u8)),
            Self::Int8(values) => values.push(integer_value!(node, place, target, i8)),
            Self::UInt16(values) => values.push(integer_value!(node, place, target, u16)),
            Self::Int16(values) => values.push(integer_value!(node, place, target, i16)),
            Self::UInt32(values) => values.push(integer_value!(node, place, target, u32)),
            Self::Int32(values) => values.push(integer_value!(node, place, target, i32)),
            Self::UInt64(values) => values.push(integer_value!(node, place, target, u64)),
            Self::Int64(values) => values.push(integer_value!(node, place, target, i64)),
            Self::Float32(values) => {
                let value = match node {
                    Node::Static(StaticNode::I64(value)) => (*value).approx_into::<f32>(),
                    Node::Static(StaticNode::U64(value)) => (*value).approx_into::<f32>(),
                    Node::Static(StaticNode::F64(value)) => (*value).approx_into::<f32>(),
                    node => return Err(Self::mismatch(node, target, place)),
                };
                // A finite JSON number beyond the F32 range rounds to an infinity.
                if value.is_infinite() {
                    return Err(ValueDefect::OutOfRange {
                        place,
                        target: target.clone(),
                    });
                }
                values.push(value);
            }
            Self::Float64(values) => {
                let value = match node {
                    Node::Static(StaticNode::I64(value)) => (*value).approx_into::<f64>(),
                    Node::Static(StaticNode::U64(value)) => (*value).approx_into::<f64>(),
                    Node::Static(StaticNode::F64(value)) => *value,
                    node => return Err(Self::mismatch(node, target, place)),
                };
                values.push(value);
            }
            Self::Vec {
                element,
                offsets,
                elements,
            } => {
                let Node::Array { len, .. } = node else {
                    return Err(Self::mismatch(node, target, place));
                };
                elements.append_elements(document, index, *len, element)?;
                offsets.push(Self::next_offset(elements.len())?);
            }
            Self::Array {
                element,
                len: declared,
                width,
                elements,
            } => {
                let Node::Array { len, .. } = node else {
                    return Err(Self::mismatch(node, target, place));
                };
                if *len != *width {
                    return Err(ValueDefect::ArrayLength {
                        place,
                        found: *len,
                        declared: *declared,
                    });
                }
                elements.append_elements(document, index, *len, element)?;
            }
        }
        Ok(())
    }

    /// Appends the `len` elements of the array at tape index `array`, each read as `target`.
    fn append_elements(
        &mut self,
        document: &Document<'_, '_>,
        array: usize,
        len: usize,
        target: &Arc<JsonTarget>,
    ) -> Result<(), ValueDefect> {
        let mut element = array + 1;
        for _ in 0..len {
            self.append(document, element, target, JsonPlace::Element)?;
            element += node_span(document.node(element));
        }
        Ok(())
    }

    fn mismatch(node: &Node<'_>, target: &Arc<JsonTarget>, place: JsonPlace) -> ValueDefect {
        ValueDefect::TypeMismatch {
            place,
            found: node_kind(node),
            expected: target.clone(),
        }
    }

    /// The Arrow array of the values, with `nulls` over them.
    fn finish(self, nulls: Option<arrow_buffer::NullBuffer>) -> ArrayRef {
        match self {
            Self::Bool(mut values) => {
                std::sync::Arc::new(BooleanArray::new(values.finish(), nulls))
            }
            Self::String { offsets, bytes } => {
                let offsets = OffsetBuffer::new(ScalarBuffer::from(offsets));
                let array = StringArray::try_new(offsets, Buffer::from_vec(bytes), nulls)
                    .verified("the column wrote increasing offsets over whole UTF-8 strings");
                std::sync::Arc::new(array)
            }
            Self::UInt8(values) => std::sync::Arc::new(UInt8Array::new(values.into(), nulls)),
            Self::Int8(values) => std::sync::Arc::new(Int8Array::new(values.into(), nulls)),
            Self::UInt16(values) => std::sync::Arc::new(UInt16Array::new(values.into(), nulls)),
            Self::Int16(values) => std::sync::Arc::new(Int16Array::new(values.into(), nulls)),
            Self::UInt32(values) => std::sync::Arc::new(UInt32Array::new(values.into(), nulls)),
            Self::Int32(values) => std::sync::Arc::new(Int32Array::new(values.into(), nulls)),
            Self::UInt64(values) => std::sync::Arc::new(UInt64Array::new(values.into(), nulls)),
            Self::Int64(values) => std::sync::Arc::new(Int64Array::new(values.into(), nulls)),
            Self::Float32(values) => std::sync::Arc::new(Float32Array::new(values.into(), nulls)),
            Self::Float64(values) => std::sync::Arc::new(Float64Array::new(values.into(), nulls)),
            Self::Vec {
                element,
                offsets,
                elements,
            } => {
                let values = elements.finish(None);
                let offsets = OffsetBuffer::new(ScalarBuffer::from(offsets));
                let array = ListArray::try_new(element.element_field(), offsets, values, nulls)
                    .verified("the column wrote increasing offsets over its own elements");
                std::sync::Arc::new(array)
            }
            Self::Array {
                element,
                len,
                elements,
                ..
            } => {
                let values = elements.finish(None);
                let array = FixedSizeListArray::try_new(
                    element.element_field(),
                    JsonTarget::fixed_width(len),
                    values,
                    nulls,
                )
                .verified("the column wrote exactly its width of elements for every value");
                std::sync::Arc::new(array)
            }
        }
    }
}

/// One output of a scan being built.
enum ScanColumn {
    /// The values an output reads as `target`.
    Value {
        target: Arc<JsonTarget>,
        values: ValueColumn,
        validity: NullBufferBuilder,
    },
    Exists {
        values: BooleanBufferBuilder,
        validity: NullBufferBuilder,
    },
}

impl ScanColumn {
    fn new(output: &JsonOutput, rows: usize) -> Self {
        match output {
            JsonOutput::Value { target, .. } => Self::Value {
                target: target.clone(),
                values: ValueColumn::new(target),
                validity: NullBufferBuilder::new(rows),
            },
            JsonOutput::Exists => Self::Exists {
                values: BooleanBufferBuilder::new(rows),
                validity: NullBufferBuilder::new(rows),
            },
        }
    }

    fn push_null(&mut self) {
        match self {
            Self::Value {
                values, validity, ..
            } => {
                values.push_placeholder();
                validity.append_null();
            }
            Self::Exists { values, validity } => {
                values.append(false);
                validity.append_null();
            }
        }
    }

    fn finish(self) -> ArrayRef {
        match self {
            Self::Value {
                values,
                mut validity,
                ..
            } => values.finish(validity.finish()),
            Self::Exists {
                mut values,
                mut validity,
            } => std::sync::Arc::new(BooleanArray::new(values.finish(), validity.finish())),
        }
    }
}

/// Parses documents one after another, reusing its buffers from one document to the next.
struct DocumentReader {
    /// A copy of the document, which simd-json unescapes strings into.
    text: Vec<u8>,
    buffers: Buffers,
    open_ends: Vec<usize>,
}

/// Why a document as a whole could not be read.
#[derive(Debug, Clone, Copy)]
enum DocumentDefect {
    Malformed,
    TooLarge,
    TooDeep,
}

impl DocumentDefect {
    const fn for_operation(self, operation: JsonOperation) -> JsonDefect {
        match self {
            Self::Malformed => JsonDefect::Malformed(operation),
            Self::TooLarge => JsonDefect::DocumentTooLarge(operation),
            Self::TooDeep => JsonDefect::DocumentTooDeep(operation),
        }
    }
}

impl DocumentReader {
    fn new() -> Self {
        Self {
            text: Vec::new(),
            buffers: Buffers::default(),
            open_ends: Vec::new(),
        }
    }
}

/// Answers every one of `outputs` for each row of `documents` that `selected` holds, parsing each
/// document once. A row `selected` does not hold, and a null document, answer a null. The result
/// holds one column for each output, in order, over the documents' rows.
///
/// A defect is recorded against the output that found it, at that output's span, when the output
/// reports defects; a defect of a column being built is recorded whatever the output.
pub(crate) fn scan(
    documents: &StringArray,
    selected: impl Fn(usize) -> bool,
    outputs: &[JsonScanOutput],
    row_errors: &mut RowErrors,
) -> StructArray {
    let rows = documents.len();
    let mut columns = Vec::with_capacity(outputs.len());
    for output in outputs {
        columns.push(ScanColumn::new(&output.extraction.output, rows));
    }
    let mut reader = DocumentReader::new();
    for row in 0..rows {
        if !selected(row) || documents.is_null(row) {
            for column in &mut columns {
                column.push_null();
            }
            continue;
        }
        let text = documents.value(row);
        reader.answer(text, row, outputs, &mut columns, row_errors);
    }

    let mut fields = Vec::with_capacity(outputs.len());
    let mut arrays = Vec::with_capacity(outputs.len());
    for (position, column) in columns.into_iter().enumerate() {
        let array = column.finish();
        fields.push(Field::new(
            position.to_string(),
            array.data_type().clone(),
            true,
        ));
        arrays.push(array);
    }
    StructArray::try_new_with_length(Fields::from(fields), arrays, None, rows)
        .verified("every scan column holds one value for each document")
}

impl DocumentReader {
    /// Parses one document and appends each output's answer for it.
    fn answer(
        &mut self,
        text: &str,
        row: usize,
        outputs: &[JsonScanOutput],
        columns: &mut [ScanColumn],
        row_errors: &mut RowErrors,
    ) {
        if text.len() > MAX_DOCUMENT_BYTES {
            Self::fail_document(DocumentDefect::TooLarge, row, outputs, columns, row_errors);
            return;
        }
        self.text.clear();
        self.text.extend_from_slice(text.as_bytes());
        let Ok(tape) = simd_json::to_tape_with_buffers(&mut self.text, &mut self.buffers) else {
            Self::fail_document(DocumentDefect::Malformed, row, outputs, columns, row_errors);
            return;
        };
        let document = Document { tape: &tape.0 };
        if document.nests_deeper_than(MAX_DOCUMENT_DEPTH, &mut self.open_ends) {
            Self::fail_document(DocumentDefect::TooDeep, row, outputs, columns, row_errors);
            return;
        }
        for (output, column) in outputs.iter().zip(columns.iter_mut()) {
            Self::answer_output(&document, row, output, column, row_errors);
        }
    }

    fn answer_output(
        document: &Document<'_, '_>,
        row: usize,
        output: &JsonScanOutput,
        column: &mut ScanColumn,
        row_errors: &mut RowErrors,
    ) {
        let extraction = &output.extraction;
        let located = document.locate(&extraction.path);
        match column {
            ScanColumn::Exists { values, validity } => {
                values.append(located.is_some());
                validity.append_non_null();
            }
            ScanColumn::Value {
                target,
                values,
                validity,
            } => {
                let Some(index) = located else {
                    values.push_placeholder();
                    validity.append_null();
                    return;
                };
                if let Node::Static(StaticNode::Null) = document.node(index) {
                    values.push_placeholder();
                    validity.append_null();
                    return;
                }
                let mark = values.len();
                match values.append(document, index, target, JsonPlace::Value) {
                    Ok(()) => validity.append_non_null(),
                    Err(defect) => {
                        values.truncate(mark);
                        values.push_placeholder();
                        validity.append_null();
                        let defect = defect.into_defect(extraction, target);
                        let reported =
                            extraction.output.reports_defects() || !defect.concerns_the_document();
                        if reported {
                            row_errors.push(row, Self::side_error(defect, output.span));
                        }
                    }
                }
            }
        }
    }

    fn fail_document(
        defect: DocumentDefect,
        row: usize,
        outputs: &[JsonScanOutput],
        columns: &mut [ScanColumn],
        row_errors: &mut RowErrors,
    ) {
        for (output, column) in outputs.iter().zip(columns.iter_mut()) {
            column.push_null();
            if output.extraction.output.reports_defects() {
                let defect = defect.for_operation(output.extraction.output.operation());
                row_errors.push(row, Self::side_error(defect, output.span));
            }
        }
    }

    fn side_error(defect: JsonDefect, span: Span) -> SideError {
        SideError {
            reason: SideErrorReason::Json(defect),
            span,
        }
    }
}

#[cfg(test)]
#[path = "json_tests.rs"]
mod tests;
