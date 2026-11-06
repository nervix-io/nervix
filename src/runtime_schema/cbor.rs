//! Schemaful CBOR wire values and their direct Arrow conversion.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** CBOR value validation, typed Arrow decoding and native byte-string encoding.
//! - **Depends on.** Compiled codec models, Arrow builders and ciborium.
//! - **Must not know.** Domains, graph placement, connector lifecycle or JSON interchange.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use std::{io, io::Cursor};

use ahash::{HashMap, HashMapExt as _, HashSetExt as _};
use arch_into::ArchInto as _;
use arrow_array::builder::{
    ArrayBuilder, BinaryBuilder, BooleanBuilder, FixedSizeListBuilder, Float32Builder,
    Float64Builder, Int8Builder, Int16Builder, Int32Builder, Int64Builder, ListBuilder,
    StringBuilder, TimestampNanosecondBuilder, UInt8Builder, UInt16Builder, UInt32Builder,
    UInt64Builder,
};
use arrow_schema::DataType as ArrowDataType;
use chrono::DateTime;
use ciborium::value::Value;
use ciborium_ll::{Decoder as CborDecoder, Header as CborHeader};
use error_stack::{Report, ResultExt as _};
use nervix_approx_into::ApproxInto as _;
use nervix_models::{CborType, CreateWireSchema, ParseAsType, WireSchemaStrictness};
use serde::{
    Serialize, Serializer,
    ser::{SerializeMap, SerializeSeq},
};

use super::{
    ArrowCodecRow, ArrowCodecSequence, ArrowCodecValue, CborHeaderScanError, CodecContractError,
    CodecError, CompiledCodec, FieldDecodeError, RuntimeRecordBatchBuilder, RuntimeSchemaError,
    RuntimeValueLocationRef, close_partial_fixed_size_list, close_partial_list,
    sequence_element_data_type, typed_arrow_builder,
};

#[derive(Debug, Clone)]
pub(super) struct CompiledCborWireSchema {
    strictness: WireSchemaStrictness,
    fields: HashMap<String, CompiledCborWireField>,
}

#[derive(Debug, Clone, Copy)]
struct CompiledCborWireField {
    ty: CborType,
    optional: bool,
}

impl CompiledCborWireSchema {
    pub(super) fn compile(schema: &CreateWireSchema<CborType>) -> Self {
        Self {
            strictness: schema.strictness,
            fields: schema
                .fields
                .iter()
                .map(|field| {
                    (
                        field.name.as_str().to_string(),
                        CompiledCborWireField {
                            ty: field.ty,
                            optional: field.optional,
                        },
                    )
                })
                .collect(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
pub enum CborValueKind {
    #[strum(serialize = "integer")]
    Integer,
    #[strum(serialize = "float")]
    Float,
    #[strum(serialize = "boolean")]
    Boolean,
    #[strum(serialize = "text")]
    Text,
    #[strum(serialize = "bytes")]
    Bytes,
    #[strum(serialize = "array")]
    Array,
    #[strum(serialize = "map")]
    Map,
    #[strum(serialize = "null")]
    Null,
    #[strum(serialize = "tag")]
    Tag,
}

impl From<ciborium_ll::Error<io::Error>> for CborHeaderScanError {
    fn from(error: ciborium_ll::Error<io::Error>) -> Self {
        match error {
            ciborium_ll::Error::Io(source) => Self::Read(source),
            ciborium_ll::Error::Syntax(offset) => Self::Syntax(offset),
        }
    }
}

type ScanResult<T> = error_stack::Result<T, CborHeaderScanError>;

fn scan_error(source: ciborium_ll::Error<io::Error>) -> Report<CborHeaderScanError> {
    Report::new(source.into())
}

fn kind(value: &Value) -> CborValueKind {
    match value {
        Value::Integer(_) => CborValueKind::Integer,
        Value::Float(_) => CborValueKind::Float,
        Value::Bool(_) => CborValueKind::Boolean,
        Value::Text(_) => CborValueKind::Text,
        Value::Bytes(_) => CborValueKind::Bytes,
        Value::Array(_) => CborValueKind::Array,
        Value::Map(_) => CborValueKind::Map,
        Value::Null => CborValueKind::Null,
        Value::Tag(_, _) => CborValueKind::Tag,
        _ => CborValueKind::Tag,
    }
}

fn matches_type(value: &Value, ty: CborType) -> bool {
    matches!(
        (value, ty),
        (Value::Integer(_), CborType::Integer)
            | (Value::Float(_), CborType::Float)
            | (Value::Bool(_), CborType::Boolean)
            | (Value::Text(_), CborType::Text)
            | (Value::Bytes(_), CborType::Bytes)
            | (Value::Array(_), CborType::Array)
    )
}

/// Check all nested values before an unknown field is ignored in loose mode. A tag or a CBOR map
/// with ambiguous keys is never a value in the schemaful wire contract.
fn validate_value(
    codec: &CompiledCodec,
    field: &str,
    value: &Value,
) -> error_stack::Result<(), CodecError> {
    match value {
        Value::Tag(_, _) => Err(codec.field_decode_failure(field, FieldDecodeError::CborTag)),
        Value::Array(values) => {
            for value in values {
                validate_value(codec, field, value)?;
            }
            Ok(())
        }
        Value::Map(entries) => {
            let mut keys = ahash::HashSet::with_capacity(entries.len());
            for (key, value) in entries {
                let Value::Text(key) = key else {
                    return Err(codec.field_decode_failure(field, FieldDecodeError::CborNonTextKey));
                };
                if !keys.insert(key) {
                    return Err(codec.field_decode_failure(
                        field,
                        FieldDecodeError::CborDuplicateKey { key: key.clone() },
                    ));
                }
                validate_value(codec, field, value)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

pub(super) fn decode(
    codec: &CompiledCodec,
    wire_schema: &CompiledCborWireSchema,
    payload: &[u8],
    builder: &mut RuntimeRecordBatchBuilder,
) -> error_stack::Result<(), CodecError> {
    // ciborium turns short bignum tags 2 and 3 into integers before Value can retain them.
    // Inspect tagged input at the CBOR header boundary so every tag stays a type mismatch.
    let scanned = bignum_tag_field(payload);
    if let Ok(Some(field)) = &scanned {
        return Err(codec.field_decode_failure(field, FieldDecodeError::CborTag));
    }
    let value = parse_value(codec, payload)?;
    if let Err(source) = scanned {
        return Err(source.change_context(CodecError::CborHeaderScan {
            codec: codec.name.as_str().to_string(),
        }));
    }
    if matches!(value, Value::Tag(_, _)) {
        return Err(codec.field_decode_failure("<root>", FieldDecodeError::CborTag));
    }
    let Value::Map(entries) = &value else {
        return Err(Report::new(CodecError::ExpectedObject {
            codec: codec.name.as_str().to_string(),
        }));
    };

    let mut fields = HashMap::with_capacity(entries.len());
    for (key, value) in entries {
        let Value::Text(key) = key else {
            return Err(codec.field_decode_failure("<map key>", FieldDecodeError::CborNonTextKey));
        };
        if fields.insert(key.as_str(), value).is_some() {
            return Err(codec.field_decode_failure(
                key,
                FieldDecodeError::CborDuplicateKey { key: key.clone() },
            ));
        }
        if !wire_schema.strictness.allows_unknown_fields() && !wire_schema.fields.contains_key(key)
        {
            return Err(Report::new(CodecError::UnexpectedField {
                codec: codec.name.as_str().to_string(),
                field: key.clone(),
            }));
        }
        validate_value(codec, key, value)?;
    }

    for field in codec.schema.fields() {
        let Some(wire_field) = wire_schema.fields.get(&field.name) else {
            return Err(
                codec.contract_violation(CodecContractError::MissingWireField {
                    field: field.name.clone(),
                }),
            );
        };
        let Some(value) = fields.get(field.name.as_str()).copied() else {
            if field.optional && wire_field.optional {
                builder
                    .append_null()
                    .change_context_lazy(|| codec.invalid())?;
                continue;
            }
            return Err(Report::new(CodecError::MissingField {
                codec: codec.name.as_str().to_string(),
                field: field.name.clone(),
            }));
        };
        if matches!(value, Value::Null) {
            if field.optional && wire_field.optional {
                builder
                    .append_null()
                    .change_context_lazy(|| codec.invalid())?;
                continue;
            }
            return Err(codec.field_decode_failure(&field.name, FieldDecodeError::RequiredNull));
        }
        if !matches_type(value, wire_field.ty) {
            return Err(codec.field_decode_failure(
                &field.name,
                FieldDecodeError::CborWireType {
                    expected: wire_field.ty,
                    found: kind(value),
                },
            ));
        }
        builder
            .append_cbor_value(value)
            .change_context_lazy(|| codec.parse_field(&field.name))?;
    }
    Ok(())
}

fn parse_value(codec: &CompiledCodec, payload: &[u8]) -> error_stack::Result<Value, CodecError> {
    ciborium::from_reader::<Value, _>(Cursor::new(payload)).map_err(|source| {
        Report::new(CodecError::CborDecode {
            codec: codec.name.as_str().to_string(),
            source,
        })
    })
}

fn bignum_tag_field(payload: &[u8]) -> ScanResult<Option<String>> {
    if !payload.iter().enumerate().any(|(index, byte)| match byte {
        0xc2 | 0xc3 => true,
        0xd8 => matches!(payload.get(index + 1).copied(), Some(2 | 3)),
        0xd9 => matches!(payload.get(index + 1..index + 3), Some([0, 2 | 3])),
        0xda => matches!(payload.get(index + 1..index + 5), Some([0, 0, 0, 2 | 3])),
        0xdb => matches!(
            payload.get(index + 1..index + 9),
            Some([0, 0, 0, 0, 0, 0, 0, 2 | 3])
        ),
        _ => false,
    }) {
        return Ok(None);
    }
    let mut decoder = CborDecoder::from(payload);
    scan_item(&mut decoder, "<root>", true, 0)
}

fn scan_item(
    decoder: &mut CborDecoder<&[u8]>,
    field: &str,
    root: bool,
    depth: usize,
) -> ScanResult<Option<String>> {
    if depth >= 512 {
        return Err(Report::new(CborHeaderScanError::Depth));
    }
    let header = decoder.pull().map_err(scan_error)?;
    scan_header(decoder, header, field, root, depth)
}

fn scan_header(
    decoder: &mut CborDecoder<&[u8]>,
    header: CborHeader,
    field: &str,
    root: bool,
    depth: usize,
) -> ScanResult<Option<String>> {
    match header {
        CborHeader::Tag(2 | 3) => Ok(Some(field.to_string())),
        CborHeader::Tag(_) => scan_item(decoder, field, root, depth + 1),
        CborHeader::Array(len) => scan_array(decoder, len, field, depth + 1),
        CborHeader::Map(len) => scan_map(decoder, len, field, root, depth + 1),
        CborHeader::Bytes(len) => {
            drain_bytes(decoder, len)?;
            Ok(None)
        }
        CborHeader::Text(len) => {
            drain_text(decoder, len, None)?;
            Ok(None)
        }
        CborHeader::Break => Err(Report::new(CborHeaderScanError::Break(decoder.offset()))),
        CborHeader::Positive(_)
        | CborHeader::Negative(_)
        | CborHeader::Float(_)
        | CborHeader::Simple(_) => Ok(None),
    }
}

fn scan_array(
    decoder: &mut CborDecoder<&[u8]>,
    len: Option<usize>,
    field: &str,
    depth: usize,
) -> ScanResult<Option<String>> {
    let mut remaining = len.unwrap_or(usize::MAX);
    while remaining > 0 {
        let found = if len.is_none() {
            let header = decoder.pull().map_err(scan_error)?;
            if header == CborHeader::Break {
                break;
            }
            scan_header(decoder, header, field, false, depth)?
        } else {
            scan_item(decoder, field, false, depth)?
        };
        if let Some(tagged_field) = found {
            return Ok(Some(tagged_field));
        }
        remaining -= 1;
    }
    Ok(None)
}

fn scan_map(
    decoder: &mut CborDecoder<&[u8]>,
    len: Option<usize>,
    field: &str,
    root: bool,
    depth: usize,
) -> ScanResult<Option<String>> {
    let mut remaining = len.unwrap_or(usize::MAX);
    while remaining > 0 {
        let key = decoder.pull().map_err(scan_error)?;
        if len.is_none() && key == CborHeader::Break {
            break;
        }
        let value_field = if root {
            if let CborHeader::Text(len) = key {
                let mut name = String::new();
                drain_text(decoder, len, Some(&mut name))?;
                name
            } else {
                if let Some(tagged_field) = scan_header(decoder, key, "<map key>", false, depth)? {
                    return Ok(Some(tagged_field));
                }
                "<map key>".to_string()
            }
        } else {
            if let Some(tagged_field) = scan_header(decoder, key, field, false, depth)? {
                return Ok(Some(tagged_field));
            }
            field.to_string()
        };
        if let Some(tagged_field) = scan_item(decoder, &value_field, false, depth)? {
            return Ok(Some(tagged_field));
        }
        remaining -= 1;
    }
    Ok(None)
}

fn drain_bytes(decoder: &mut CborDecoder<&[u8]>, len: Option<usize>) -> ScanResult<()> {
    let mut segments = decoder.bytes(len);
    let mut buffer = [0; 256];
    while let Some(mut segment) = segments.pull().map_err(scan_error)? {
        while segment.pull(&mut buffer).map_err(scan_error)?.is_some() {}
    }
    Ok(())
}

fn drain_text(
    decoder: &mut CborDecoder<&[u8]>,
    len: Option<usize>,
    mut capture: Option<&mut String>,
) -> ScanResult<()> {
    let mut segments = decoder.text(len);
    let mut buffer = [0; 256];
    while let Some(mut segment) = segments.pull().map_err(scan_error)? {
        while let Some(chunk) = segment.pull(&mut buffer).map_err(scan_error)? {
            if let Some(name) = capture.as_mut() {
                name.push_str(chunk);
            }
        }
    }
    Ok(())
}

pub(super) fn write_row<W: io::Write>(
    codec: &CompiledCodec,
    row: &ArrowCodecRow<'_>,
    output: &mut W,
) -> error_stack::Result<(), CodecError> {
    ciborium::into_writer(&CborRow(row), output).map_err(|source| {
        Report::new(CodecError::CborEncode {
            codec: codec.name.as_str().to_string(),
            source,
        })
    })
}

struct CborRow<'a, 'b>(&'a ArrowCodecRow<'b>);

impl Serialize for CborRow<'_, '_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        for (index, field) in self.0.codec.schema.fields.iter().enumerate() {
            let value = self.0.value(index);
            if value.is_null() && field.optional {
                continue;
            }
            map.serialize_entry(&field.name, &CborArrowValue(value))?;
        }
        map.end()
    }
}

struct CborArrowValue<'a>(ArrowCodecValue<'a>);

impl Serialize for CborArrowValue<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if self.0.is_null() {
            return Err(serde::ser::Error::custom(format!(
                "field '{}' contains null at row {}",
                self.0.field, self.0.row_index
            )));
        }
        match self.0.ty {
            ParseAsType::Bytes => {
                let array = self
                    .0
                    .typed::<arrow_array::BinaryArray>()
                    .map_err(serde::ser::Error::custom)?;
                serializer.serialize_bytes(array.value(self.0.row_index))
            }
            ParseAsType::Array { .. } | ParseAsType::Vec { .. } => {
                let sequence = self.0.sequence().map_err(serde::ser::Error::custom)?;
                CborArrowSequence(sequence).serialize(serializer)
            }
            _ => self.0.serialize(serializer),
        }
    }
}

struct CborArrowSequence<'a>(ArrowCodecSequence<'a>);

impl Serialize for CborArrowSequence<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.0.rows.len()))?;
        for row_index in self.0.rows.clone() {
            sequence.serialize_element(&CborArrowValue(ArrowCodecValue {
                codec: self.0.codec,
                array: self.0.array,
                ty: self.0.element,
                field: self.0.field,
                row_index,
            }))?;
        }
        sequence.end()
    }
}

#[cfg_attr(
    nervix_lint,
    nervix::dispatch(
        reason = "integer TryFrom applies a deterministic range check to CBOR values"
    )
)]
fn integer_as<T: TryFrom<i128>>(value: &Value) -> Option<T> {
    let Value::Integer(integer) = value else {
        return None;
    };
    T::try_from(i128::from(*integer)).ok()
}

pub(super) fn append_to_arrow(
    builder: &mut dyn ArrayBuilder,
    ty: &ParseAsType,
    expected: &ArrowDataType,
    value: &Value,
    location: &RuntimeValueLocationRef<'_>,
) -> error_stack::Result<(), RuntimeSchemaError> {
    let incompatible = || {
        Report::new(RuntimeSchemaError::CborValueTypeMismatch {
            location: location.to_runtime_location(),
            expected: ty.clone(),
            found: kind(value),
        })
    };
    macro_rules! append_primitive {
        ($builder:ty, $parsed:expr) => {{
            let Some(parsed) = ($parsed) else {
                return Err(incompatible());
            };
            typed_arrow_builder::<$builder>(builder, expected, location)?.append_value(parsed);
            Ok(())
        }};
    }
    match ty {
        ParseAsType::U8 => append_primitive!(UInt8Builder, integer_as::<u8>(value)),
        ParseAsType::I8 => append_primitive!(Int8Builder, integer_as::<i8>(value)),
        ParseAsType::U16 => append_primitive!(UInt16Builder, integer_as::<u16>(value)),
        ParseAsType::I16 => append_primitive!(Int16Builder, integer_as::<i16>(value)),
        ParseAsType::U32 => append_primitive!(UInt32Builder, integer_as::<u32>(value)),
        ParseAsType::I32 => append_primitive!(Int32Builder, integer_as::<i32>(value)),
        ParseAsType::U64 => append_primitive!(UInt64Builder, integer_as::<u64>(value)),
        ParseAsType::I64 => append_primitive!(Int64Builder, integer_as::<i64>(value)),
        ParseAsType::Bool => append_primitive!(
            BooleanBuilder,
            if let Value::Bool(v) = value {
                Some(*v)
            } else {
                None
            }
        ),
        ParseAsType::String => append_primitive!(
            StringBuilder,
            if let Value::Text(v) = value {
                Some(v.as_str())
            } else {
                None
            }
        ),
        ParseAsType::Bytes => {
            let Value::Bytes(bytes) = value else {
                return Err(incompatible());
            };
            typed_arrow_builder::<BinaryBuilder>(builder, expected, location)?.append_value(bytes);
            Ok(())
        }
        ParseAsType::Datetime => append_primitive!(
            TimestampNanosecondBuilder,
            if let Value::Text(v) = value {
                DateTime::parse_from_rfc3339(v)
                    .ok()
                    .and_then(|v| v.timestamp_nanos_opt())
            } else {
                None
            }
        ),
        ParseAsType::F32 => append_primitive!(
            Float32Builder,
            if let Value::Float(v) = value {
                Some((*v).approx_into())
            } else {
                None
            }
        ),
        ParseAsType::F64 => append_primitive!(
            Float64Builder,
            if let Value::Float(v) = value {
                Some(*v)
            } else {
                None
            }
        ),
        ParseAsType::Array { element, len } => {
            let Value::Array(values) = value else {
                return Err(incompatible());
            };
            if values.len() != len.get().arch_into() {
                return Err(Report::new(
                    RuntimeSchemaError::RuntimeArrayLengthMismatch {
                        location: location.to_runtime_location(),
                        expected: *len,
                        found: values.len(),
                    },
                ));
            }
            let element_expected = sequence_element_data_type(expected, ty, location)?;
            let builder = typed_arrow_builder::<FixedSizeListBuilder<Box<dyn ArrayBuilder>>>(
                builder, expected, location,
            )?;
            for (index, value) in values.iter().enumerate() {
                let element_location = RuntimeValueLocationRef::Element {
                    parent: location,
                    index,
                };
                if let Err(error) = append_to_arrow(
                    builder.values().as_mut(),
                    element,
                    element_expected,
                    value,
                    &element_location,
                ) {
                    close_partial_fixed_size_list(builder, element, len.get().arch_into());
                    return Err(error);
                }
            }
            builder.append(true);
            Ok(())
        }
        ParseAsType::Vec { element } => {
            let Value::Array(values) = value else {
                return Err(incompatible());
            };
            let element_expected = sequence_element_data_type(expected, ty, location)?;
            let builder = typed_arrow_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(
                builder, expected, location,
            )?;
            for (index, value) in values.iter().enumerate() {
                let element_location = RuntimeValueLocationRef::Element {
                    parent: location,
                    index,
                };
                if let Err(error) = append_to_arrow(
                    builder.values().as_mut(),
                    element,
                    element_expected,
                    value,
                    &element_location,
                ) {
                    close_partial_list(builder);
                    return Err(error);
                }
            }
            builder.append(true);
            Ok(())
        }
    }
}
