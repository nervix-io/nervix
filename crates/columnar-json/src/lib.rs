//! JSON object encoding directly from typed Arrow columns.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Prequoted keys, typed column writers, JSON escaping and recursive list encoding.
//! - **Depends on.** Arrow, the byte classifier and format libraries.
//! - **Must not know.** Codecs, connector plans, emitters, graph state or Nervix models.

use std::io;

use arrow_array::{
    Array, BinaryArray, BooleanArray, FixedSizeListArray, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, ListArray, RecordBatch, StringArray,
    TimestampNanosecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::DataType;
use base64_simd::AsOut as _;
use chrono::DateTime;
use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_simd_kernels::JsonEscapeClassification;
use thiserror::Error;

/// What a top-level JSON object does with a null field.
#[derive(Clone, Copy, Debug)]
pub enum FieldNulls {
    Omit,
    Reject,
    Write,
}

/// What a JSON list does with a null element.
#[derive(Clone, Copy, Debug)]
pub enum NestedNulls {
    Reject,
    Write,
}

/// The JSON number contract for a 32-bit Arrow float.
#[derive(Clone, Copy, Debug)]
pub enum Float32Encoding {
    Native,
    WidenedF64,
}

/// How a binary Arrow value is carried inside a JSON string.
#[derive(Clone, Copy, Debug)]
pub enum BytesEncoding {
    /// Padded standard base64, which every JSON reader takes as text.
    Base64,
    /// The octets themselves, escaping only the quote, the backslash and the control octets.
    /// Octets that are not UTF-8 are written as they are, so only a reader that stores a string's
    /// octets without requiring UTF-8, such as ClickHouse's `JSONEachRow` input, reads the value
    /// back.
    Octets,
}

/// One object field, compiled once and reused for every batch encoded with that schema.
#[derive(Clone, Debug)]
pub struct JsonColumnSpec {
    name: String,
    quoted_key: Vec<u8>,
    nulls: FieldNulls,
    float32_encoding: Float32Encoding,
    bytes_encoding: BytesEncoding,
}

impl JsonColumnSpec {
    pub fn new(name: &str, nulls: FieldNulls) -> Self {
        let quoted_key = serde_json::to_vec(name)
            .assured("a UTF-8 string serialized into Vec<u8> has no failing output operation");
        Self {
            name: name.to_string(),
            quoted_key,
            nulls,
            float32_encoding: Float32Encoding::Native,
            bytes_encoding: BytesEncoding::Base64,
        }
    }

    pub fn with_float32_encoding(mut self, encoding: Float32Encoding) -> Self {
        self.float32_encoding = encoding;
        self
    }

    /// How this field's binary values, and the binary elements of its lists, are written.
    pub fn with_bytes_encoding(mut self, encoding: BytesEncoding) -> Self {
        self.bytes_encoding = encoding;
        self
    }
}

/// A column cannot be prepared for typed JSON emission.
#[derive(Debug, Error)]
pub enum JsonColumnError {
    #[error("JSON has {fields} field descriptions for {columns} Arrow columns")]
    ColumnCount { fields: usize, columns: usize },
    #[error("JSON column '{column}' has unsupported Arrow type {data_type}")]
    Unsupported { column: String, data_type: DataType },
    #[error("JSON string column '{column}' has invalid Arrow offsets")]
    StringOffsets { column: String },
}

/// A selected row cannot be written as a JSON object.
#[derive(Debug, Error)]
pub enum JsonWriteError {
    #[error("JSON row {row} is outside a batch of {rows} rows")]
    RowOutOfBounds { row: usize, rows: usize },
    #[error("JSON field '{field}' contains null at row {row}")]
    RequiredNull { field: String, row: usize },
    #[error("failed to escape a JSON string: {source}")]
    StringEscape {
        #[source]
        source: serde_json::Error,
    },
    #[error("failed to write a JSON number: {source}")]
    NumberWrite {
        #[source]
        source: serde_json::Error,
    },
    #[error("failed to write JSON bytes: {source}")]
    Write {
        #[source]
        source: io::Error,
    },
}

macro_rules! write_json_bytes {
    ($write:expr) => {
        $write.map_err(|source| Report::new(JsonWriteError::Write { source }))?
    };
}

/// The columns of one batch, downcast and classified once before any row is encoded.
pub struct JsonColumns<'a> {
    fields: Vec<JsonField<'a>>,
    rows: usize,
    nested_nulls: NestedNulls,
}

struct JsonField<'a> {
    spec: &'a JsonColumnSpec,
    column: JsonColumn<'a>,
}

enum JsonColumn<'a> {
    Bool(&'a BooleanArray),
    U8(&'a UInt8Array),
    I8(&'a Int8Array),
    U16(&'a UInt16Array),
    I16(&'a Int16Array),
    U32(&'a UInt32Array),
    I32(&'a Int32Array),
    U64(&'a UInt64Array),
    I64(&'a Int64Array),
    F32(&'a Float32Array),
    F64(&'a Float64Array),
    String {
        values: &'a StringArray,
        escapes: JsonEscapeClassification,
    },
    Bytes {
        values: &'a BinaryArray,
        encoding: BytesEncoding,
    },
    Datetime(&'a TimestampNanosecondArray),
    List {
        offsets: &'a ListArray,
        elements: Box<JsonColumn<'a>>,
    },
    FixedList {
        offsets: &'a FixedSizeListArray,
        elements: Box<JsonColumn<'a>>,
    },
}

#[derive(Debug, Error)]
enum ColumnBuildError {
    #[error("unsupported Arrow type {0}")]
    Unsupported(DataType),
    #[error("invalid Arrow string offsets")]
    StringOffsets,
}

impl<'a> JsonColumns<'a> {
    pub fn new(
        batch: &'a RecordBatch,
        specs: &'a [JsonColumnSpec],
        nested_nulls: NestedNulls,
    ) -> error_stack::Result<Self, JsonColumnError> {
        if specs.len() != batch.num_columns() {
            return Err(Report::new(JsonColumnError::ColumnCount {
                fields: specs.len(),
                columns: batch.num_columns(),
            }));
        }
        let mut fields = Vec::with_capacity(specs.len());
        for (spec, array) in specs.iter().zip(batch.columns()) {
            let column =
                JsonColumn::new(array.as_ref(), spec.bytes_encoding).map_err(|report| {
                    let context = match report.current_context() {
                        ColumnBuildError::Unsupported(data_type) => JsonColumnError::Unsupported {
                            column: spec.name.clone(),
                            data_type: data_type.clone(),
                        },
                        ColumnBuildError::StringOffsets => JsonColumnError::StringOffsets {
                            column: spec.name.clone(),
                        },
                    };
                    report.change_context(context)
                })?;
            fields.push(JsonField { spec, column });
        }
        Ok(Self {
            fields,
            rows: batch.num_rows(),
            nested_nulls,
        })
    }

    /// Writes exactly one selected row. Each field is read from its typed column; only string
    /// cells that the values-buffer classifier flagged enter an escaping serializer.
    pub fn write_row<W: io::Write>(
        &self,
        row: usize,
        output: &mut W,
    ) -> error_stack::Result<(), JsonWriteError> {
        if row >= self.rows {
            return Err(Report::new(JsonWriteError::RowOutOfBounds {
                row,
                rows: self.rows,
            }));
        }
        write_json_bytes!(output.write_all(b"{"));
        let mut first = true;
        for field in &self.fields {
            let is_null = field.column.is_null(row);
            if is_null {
                match field.spec.nulls {
                    FieldNulls::Omit => continue,
                    FieldNulls::Reject => {
                        return Err(Report::new(JsonWriteError::RequiredNull {
                            field: field.spec.name.clone(),
                            row,
                        }));
                    }
                    FieldNulls::Write => {}
                }
            }
            if !first {
                write_json_bytes!(output.write_all(b","));
            }
            first = false;
            write_json_bytes!(output.write_all(&field.spec.quoted_key));
            write_json_bytes!(output.write_all(b":"));
            if is_null {
                write_json_bytes!(output.write_all(b"null"));
            } else {
                field.column.write_value(
                    row,
                    &field.spec.name,
                    self.nested_nulls,
                    field.spec.float32_encoding,
                    output,
                )?;
            }
        }
        write_json_bytes!(output.write_all(b"}"));
        Ok(())
    }
}

impl<'a> JsonColumn<'a> {
    fn new(
        array: &'a dyn Array,
        bytes_encoding: BytesEncoding,
    ) -> error_stack::Result<Self, ColumnBuildError> {
        macro_rules! downcast {
            ($ty:ty, $variant:ident) => {
                if let Some(values) = array.as_any().downcast_ref::<$ty>() {
                    return Ok(Self::$variant(values));
                }
            };
        }
        downcast!(BooleanArray, Bool);
        downcast!(UInt8Array, U8);
        downcast!(Int8Array, I8);
        downcast!(UInt16Array, U16);
        downcast!(Int16Array, I16);
        downcast!(UInt32Array, U32);
        downcast!(Int32Array, I32);
        downcast!(UInt64Array, U64);
        downcast!(Int64Array, I64);
        downcast!(Float32Array, F32);
        downcast!(Float64Array, F64);
        downcast!(TimestampNanosecondArray, Datetime);
        if let Some(values) = array.as_any().downcast_ref::<BinaryArray>() {
            return Ok(Self::Bytes {
                values,
                encoding: bytes_encoding,
            });
        }
        if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
            let escapes =
                JsonEscapeClassification::new(values.value_data(), values.value_offsets())
                    .change_context(ColumnBuildError::StringOffsets)?;
            return Ok(Self::String { values, escapes });
        }
        if let Some(offsets) = array.as_any().downcast_ref::<ListArray>() {
            let elements = Self::new(offsets.values().as_ref(), bytes_encoding)?;
            return Ok(Self::List {
                offsets,
                elements: Box::new(elements),
            });
        }
        if let Some(offsets) = array.as_any().downcast_ref::<FixedSizeListArray>() {
            let elements = Self::new(offsets.values().as_ref(), bytes_encoding)?;
            return Ok(Self::FixedList {
                offsets,
                elements: Box::new(elements),
            });
        }
        Err(Report::new(ColumnBuildError::Unsupported(
            array.data_type().clone(),
        )))
    }

    fn is_null(&self, row: usize) -> bool {
        match self {
            Self::Bool(values) => values.is_null(row),
            Self::U8(values) => values.is_null(row),
            Self::I8(values) => values.is_null(row),
            Self::U16(values) => values.is_null(row),
            Self::I16(values) => values.is_null(row),
            Self::U32(values) => values.is_null(row),
            Self::I32(values) => values.is_null(row),
            Self::U64(values) => values.is_null(row),
            Self::I64(values) => values.is_null(row),
            Self::F32(values) => values.is_null(row),
            Self::F64(values) => values.is_null(row),
            Self::String { values, .. } => values.is_null(row),
            Self::Bytes { values, .. } => values.is_null(row),
            Self::Datetime(values) => values.is_null(row),
            Self::List { offsets, .. } => offsets.is_null(row),
            Self::FixedList { offsets, .. } => offsets.is_null(row),
        }
    }

    fn write_value<W: io::Write>(
        &self,
        row: usize,
        field: &str,
        nested_nulls: NestedNulls,
        float32_encoding: Float32Encoding,
        output: &mut W,
    ) -> error_stack::Result<(), JsonWriteError> {
        if self.is_null(row) {
            match nested_nulls {
                NestedNulls::Write => write_json_bytes!(output.write_all(b"null")),
                NestedNulls::Reject => {
                    return Err(Report::new(JsonWriteError::RequiredNull {
                        field: field.to_string(),
                        row,
                    }));
                }
            }
            return Ok(());
        }

        macro_rules! write_integer {
            ($values:expr) => {{
                let mut buffer = itoa::Buffer::new();
                write_json_bytes!(output.write_all(buffer.format($values.value(row)).as_bytes()));
            }};
        }
        macro_rules! write_float {
            ($values:expr) => {{
                let value = $values.value(row);
                if value.is_finite() {
                    let mut buffer = ryu::Buffer::new();
                    write_json_bytes!(output.write_all(buffer.format(value).as_bytes()));
                } else {
                    write_json_bytes!(output.write_all(b"null"));
                }
            }};
        }
        match self {
            Self::Bool(values) => {
                write_json_bytes!(output.write_all(if values.value(row) {
                    b"true"
                } else {
                    b"false"
                }));
            }
            Self::U8(values) => write_integer!(values),
            Self::I8(values) => write_integer!(values),
            Self::U16(values) => write_integer!(values),
            Self::I16(values) => write_integer!(values),
            Self::U32(values) => write_integer!(values),
            Self::I32(values) => write_integer!(values),
            Self::U64(values) => write_integer!(values),
            Self::I64(values) => write_integer!(values),
            Self::F32(values) => {
                let value = values.value(row);
                if !value.is_finite() {
                    write_json_bytes!(output.write_all(b"null"));
                } else {
                    match float32_encoding {
                        Float32Encoding::Native => {
                            let mut buffer = ryu::Buffer::new();
                            write_json_bytes!(output.write_all(buffer.format(value).as_bytes()));
                        }
                        Float32Encoding::WidenedF64 => {
                            serde_json::to_writer(&mut *output, &f64::from(value)).map_err(
                                |source| Report::new(JsonWriteError::NumberWrite { source }),
                            )?;
                        }
                    }
                }
            }
            Self::F64(values) => write_float!(values),
            Self::String { values, escapes } => {
                let value = values.value(row);
                if escapes.row_needs_escape(row) {
                    serde_json::to_writer(&mut *output, value)
                        .map_err(|source| Report::new(JsonWriteError::StringEscape { source }))?;
                } else {
                    write_json_bytes!(output.write_all(b"\""));
                    write_json_bytes!(output.write_all(value.as_bytes()));
                    write_json_bytes!(output.write_all(b"\""));
                }
            }
            Self::Bytes { values, encoding } => {
                write_json_bytes!(output.write_all(b"\""));
                match encoding {
                    BytesEncoding::Base64 => Self::write_base64(values.value(row), output)?,
                    BytesEncoding::Octets => Self::write_octets(values.value(row), output)?,
                }
                write_json_bytes!(output.write_all(b"\""));
            }
            Self::Datetime(values) => {
                let value = DateTime::from_timestamp_nanos(values.value(row)).fixed_offset();
                write_json_bytes!(write!(output, "\"{}\"", value.format("%+")));
            }
            Self::List { offsets, elements } => {
                let bounds = offsets.value_offsets();
                let start = usize::try_from(bounds[row])
                    .assured("Arrow list offsets are nonnegative element positions");
                let end = usize::try_from(bounds[row + 1])
                    .assured("Arrow list offsets are nonnegative element positions");
                Self::write_list(
                    elements,
                    start..end,
                    field,
                    nested_nulls,
                    float32_encoding,
                    output,
                )?;
            }
            Self::FixedList { offsets, elements } => {
                let start = usize::try_from(offsets.value_offset(row))
                    .assured("Arrow fixed-size list offsets are nonnegative element positions");
                let length = usize::try_from(offsets.value_length())
                    .assured("Arrow fixed-size list length is nonnegative");
                let end = start
                    .checked_add(length)
                    .assured("Arrow fixed-size list offsets address existing values");
                Self::write_list(
                    elements,
                    start..end,
                    field,
                    nested_nulls,
                    float32_encoding,
                    output,
                )?;
            }
        }
        Ok(())
    }

    fn write_list<W: io::Write>(
        elements: &Self,
        rows: std::ops::Range<usize>,
        field: &str,
        nested_nulls: NestedNulls,
        float32_encoding: Float32Encoding,
        output: &mut W,
    ) -> error_stack::Result<(), JsonWriteError> {
        write_json_bytes!(output.write_all(b"["));
        for (index, row) in rows.enumerate() {
            if index != 0 {
                write_json_bytes!(output.write_all(b","));
            }
            elements.write_value(row, field, nested_nulls, float32_encoding, output)?;
        }
        write_json_bytes!(output.write_all(b"]"));
        Ok(())
    }

    fn write_base64<W: io::Write>(
        bytes: &[u8],
        output: &mut W,
    ) -> error_stack::Result<(), JsonWriteError> {
        // 768 source bytes produce exactly 1024 output bytes without intermediate padding.
        let mut scratch = [0_u8; 1024];
        for chunk in bytes.chunks(768) {
            let encoded_len = chunk.len().div_ceil(3) * 4;
            let encoded =
                base64_simd::STANDARD.encode_as_str(chunk, scratch[..encoded_len].as_out());
            write_json_bytes!(output.write_all(encoded.as_bytes()));
        }
        Ok(())
    }

    /// Writes `octets` as the characters of a JSON string that read back as exactly those octets.
    ///
    /// The quote and the backslash are escaped with a backslash, and a control octet as `\u00XX`,
    /// which decodes to that one octet. Every other octet is written as it is, in runs between the
    /// escaped ones.
    fn write_octets<W: io::Write>(
        octets: &[u8],
        output: &mut W,
    ) -> error_stack::Result<(), JsonWriteError> {
        const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";
        let mut run_start = 0;
        for (index, &octet) in octets.iter().enumerate() {
            let control_escape;
            let escape: &[u8] = match octet {
                b'"' => b"\\\"",
                b'\\' => b"\\\\",
                0x00..=0x1f => {
                    control_escape = [
                        b'\\',
                        b'u',
                        b'0',
                        b'0',
                        HEX_DIGITS[usize::from(octet >> 4)],
                        HEX_DIGITS[usize::from(octet & 0x0f)],
                    ];
                    &control_escape
                }
                _ => continue,
            };
            write_json_bytes!(output.write_all(&octets[run_start..index]));
            write_json_bytes!(output.write_all(escape));
            run_start = index
                .checked_add(1)
                .assured("an octet's index is below the length of the slice holding it");
        }
        write_json_bytes!(output.write_all(&octets[run_start..]));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{ArrayRef, FixedSizeListArray};
    use arrow_schema::{Field, Schema};
    use nervix_primitives::sync::StdArc;
    use serde::Serialize;

    use super::*;

    fn batch(columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
        let fields = columns
            .iter()
            .map(|(name, array)| Field::new(*name, array.data_type().clone(), true))
            .collect::<Vec<_>>();
        let values = columns.into_iter().map(|(_, array)| array).collect();
        RecordBatch::try_new(StdArc::new(Schema::new(fields)), values)
            .assured("test columns have equal row counts and matching declared types")
    }

    fn encode(
        batch: &RecordBatch,
        specs: &[JsonColumnSpec],
        row: usize,
        nested: NestedNulls,
    ) -> Vec<u8> {
        let columns = JsonColumns::new(batch, specs, nested)
            .assured("test columns use supported Arrow types");
        let mut output = Vec::new();
        columns
            .write_row(row, &mut output)
            .assured("test row has no rejected nulls");
        output
    }

    #[test]
    fn typed_columns_match_serde_json_for_clean_and_escaped_rows() {
        #[derive(Serialize)]
        struct Reference<'a> {
            #[serde(rename = "odd\"key")]
            clean: &'a str,
            escaped: &'a str,
            #[serde(skip_serializing_if = "Option::is_none")]
            note: Option<&'a str>,
            at: &'a str,
            blob: &'a str,
            tags: Vec<i32>,
            number: f64,
            enabled: bool,
        }

        let large_bytes = vec![0x6a; 1_025];
        let large_base64 = base64_simd::STANDARD.encode_to_string(&large_bytes);
        let tags = ListArray::from_iter_primitive::<arrow_array::types::Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2)]),
            Some(vec![]),
        ]);
        let batch = batch(vec![
            (
                "odd\"key",
                StdArc::new(StringArray::from(vec!["café", "plain"])),
            ),
            (
                "escaped",
                StdArc::new(StringArray::from(vec![
                    "quote \" slash \\ newline \n",
                    "clean",
                ])),
            ),
            (
                "note",
                StdArc::new(StringArray::from(vec![None, Some("present")])),
            ),
            (
                "at",
                StdArc::new(TimestampNanosecondArray::from(vec![
                    1_700_000_000_123_456_789,
                    1_700_000_000_000_000_000,
                ])),
            ),
            (
                "blob",
                StdArc::new(BinaryArray::from(vec![
                    Some(large_bytes.as_slice()),
                    Some(&b""[..]),
                ])),
            ),
            ("tags", StdArc::new(tags)),
            ("number", StdArc::new(Float64Array::from(vec![-0.0, 1.25]))),
            (
                "enabled",
                StdArc::new(BooleanArray::from(vec![true, false])),
            ),
        ]);
        let specs = [
            JsonColumnSpec::new("odd\"key", FieldNulls::Reject),
            JsonColumnSpec::new("escaped", FieldNulls::Reject),
            JsonColumnSpec::new("note", FieldNulls::Omit),
            JsonColumnSpec::new("at", FieldNulls::Reject),
            JsonColumnSpec::new("blob", FieldNulls::Reject),
            JsonColumnSpec::new("tags", FieldNulls::Reject),
            JsonColumnSpec::new("number", FieldNulls::Reject),
            JsonColumnSpec::new("enabled", FieldNulls::Reject),
        ];
        let expected = [
            Reference {
                clean: "café",
                escaped: "quote \" slash \\ newline \n",
                note: None,
                at: "2023-11-14T22:13:20.123456789+00:00",
                blob: &large_base64,
                tags: vec![1, 2],
                number: -0.0,
                enabled: true,
            },
            Reference {
                clean: "plain",
                escaped: "clean",
                note: Some("present"),
                at: "2023-11-14T22:13:20+00:00",
                blob: "",
                tags: Vec::new(),
                number: 1.25,
                enabled: false,
            },
        ];
        for (row, reference) in expected.iter().enumerate() {
            assert_eq!(
                encode(&batch, &specs, row, NestedNulls::Reject),
                serde_json::to_vec(reference).assured("test reference serializes into Vec")
            );
        }
    }

    #[test]
    fn every_integer_width_and_float_write_from_their_typed_columns() {
        let batch = batch(vec![
            ("u8", StdArc::new(UInt8Array::from(vec![255]))),
            ("i8", StdArc::new(Int8Array::from(vec![-128]))),
            ("u16", StdArc::new(UInt16Array::from(vec![65_535]))),
            ("i16", StdArc::new(Int16Array::from(vec![-32_768]))),
            ("u32", StdArc::new(UInt32Array::from(vec![u32::MAX]))),
            ("i32", StdArc::new(Int32Array::from(vec![i32::MIN]))),
            ("u64", StdArc::new(UInt64Array::from(vec![u64::MAX]))),
            ("i64", StdArc::new(Int64Array::from(vec![i64::MIN]))),
            ("f32", StdArc::new(Float32Array::from(vec![1.25]))),
            ("f64", StdArc::new(Float64Array::from(vec![f64::INFINITY]))),
        ]);
        let specs = [
            "u8", "i8", "u16", "i16", "u32", "i32", "u64", "i64", "f32", "f64",
        ]
        .map(|name| JsonColumnSpec::new(name, FieldNulls::Reject));
        assert_eq!(
            encode(&batch, &specs, 0, NestedNulls::Reject),
            br#"{"u8":255,"i8":-128,"u16":65535,"i16":-32768,"u32":4294967295,"i32":-2147483648,"u64":18446744073709551615,"i64":-9223372036854775808,"f32":1.25,"f64":null}"#
        );
    }

    #[test]
    fn widened_float32_values_match_serde_json_numbers_in_nested_columns() {
        #[derive(Serialize)]
        struct Reference {
            value: serde_json::Value,
            nested: Vec<serde_json::Value>,
        }

        let nested =
            ListArray::from_iter_primitive::<arrow_array::types::Float32Type, _, _>(vec![Some(
                vec![Some(1.2), Some(-0.0)],
            )]);
        let batch = batch(vec![
            ("value", StdArc::new(Float32Array::from(vec![1.2]))),
            ("nested", StdArc::new(nested)),
        ]);
        let specs = ["value", "nested"].map(|name| {
            JsonColumnSpec::new(name, FieldNulls::Write)
                .with_float32_encoding(Float32Encoding::WidenedF64)
        });
        let reference = Reference {
            value: serde_json::Value::from(1.2_f32),
            nested: vec![
                serde_json::Value::from(1.2_f32),
                serde_json::Value::from(-0.0_f32),
            ],
        };
        assert_eq!(
            encode(&batch, &specs, 0, NestedNulls::Write),
            serde_json::to_vec(&reference).assured("test reference serializes into Vec")
        );
    }

    #[test]
    fn clickhouse_nulls_and_nested_lists_write_explicit_nulls() {
        let list = ListArray::from_iter_primitive::<arrow_array::types::Int32Type, _, _>(vec![
            Some(vec![Some(1), None, Some(3)]),
            None,
        ]);
        let batch = batch(vec![("list", StdArc::new(list))]);
        let specs = [JsonColumnSpec::new("list", FieldNulls::Write)];
        assert_eq!(
            encode(&batch, &specs, 0, NestedNulls::Write),
            br#"{"list":[1,null,3]}"#
        );
        assert_eq!(
            encode(&batch, &specs, 1, NestedNulls::Write),
            br#"{"list":null}"#
        );

        let columns = JsonColumns::new(&batch, &specs, NestedNulls::Reject)
            .assured("test list column is supported");
        let error = match columns.write_row(0, &mut Vec::new()) {
            Ok(()) => panic!("schemaful list elements cannot be null"),
            Err(error) => error,
        };
        assert!(matches!(
            error.current_context(),
            JsonWriteError::RequiredNull { .. }
        ));
    }

    #[test]
    fn fixed_lists_and_sliced_string_offsets_encode_only_selected_rows() {
        let fixed = FixedSizeListArray::from_iter_primitive::<arrow_array::types::Int32Type, _, _>(
            vec![
                Some(vec![Some(1), Some(2)]),
                Some(vec![Some(3), Some(4)]),
                Some(vec![Some(5), Some(6)]),
            ],
            2,
        );
        let strings: ArrayRef = StdArc::new(StringArray::from(vec![
            "outside \"",
            "inside clean",
            "inside \\",
        ]));
        let strings = strings.slice(1, 2);
        let fixed: ArrayRef = StdArc::new(fixed);
        let fixed = fixed.slice(1, 2);
        let batch = batch(vec![("text", strings), ("fixed", fixed)]);
        let specs = [
            JsonColumnSpec::new("text", FieldNulls::Reject),
            JsonColumnSpec::new("fixed", FieldNulls::Reject),
        ];
        assert_eq!(
            encode(&batch, &specs, 0, NestedNulls::Reject),
            br#"{"text":"inside clean","fixed":[3,4]}"#
        );
        assert_eq!(
            encode(&batch, &specs, 1, NestedNulls::Reject),
            br#"{"text":"inside \\","fixed":[5,6]}"#
        );
    }

    #[test]
    fn octet_bytes_write_each_octet_as_itself_at_the_top_level_and_in_lists() {
        let mut elements =
            arrow_array::builder::ListBuilder::new(arrow_array::builder::BinaryBuilder::new());
        elements.values().append_value(b"\x80");
        elements.values().append_null();
        elements.append(true);
        let batch = batch(vec![
            (
                "raw",
                StdArc::new(BinaryArray::from(vec![b"\xff\x00\"\\\n\x1f~ok".as_slice()])),
            ),
            ("nested", StdArc::new(elements.finish())),
        ]);
        let specs = ["raw", "nested"].map(|name| {
            JsonColumnSpec::new(name, FieldNulls::Write).with_bytes_encoding(BytesEncoding::Octets)
        });
        let columns = JsonColumns::new(&batch, &specs, NestedNulls::Write)
            .assured("binary columns and lists of them are supported");
        let mut first = Vec::new();
        columns
            .write_row(0, &mut first)
            .assured("the first test row has no rejected nulls");

        assert_eq!(
            first,
            b"{\"raw\":\"\xff\\u0000\\\"\\\\\\u000a\\u001f~ok\",\"nested\":[\"\x80\",null]}"
                .to_vec()
        );
    }

    #[test]
    fn invalid_row_null_and_unsupported_column_are_typed_errors() {
        let null_batch = batch(vec![(
            "text",
            StdArc::new(StringArray::from(vec![None::<&str>])),
        )]);
        let specs = [JsonColumnSpec::new("text", FieldNulls::Reject)];
        let columns = JsonColumns::new(&null_batch, &specs, NestedNulls::Reject)
            .assured("string column is supported");
        let outside = columns
            .write_row(1, &mut Vec::new())
            .err()
            .assured("row one is outside a one-row batch");
        assert!(matches!(
            outside.current_context(),
            JsonWriteError::RowOutOfBounds { .. }
        ));
        let null = columns
            .write_row(0, &mut Vec::new())
            .err()
            .assured("required field is null in the test row");
        assert!(matches!(
            null.current_context(),
            JsonWriteError::RequiredNull { .. }
        ));

        let unsupported = batch(vec![("value", StdArc::new(arrow_array::NullArray::new(1)))]);
        let specs = [JsonColumnSpec::new("value", FieldNulls::Reject)];
        let unsupported = JsonColumns::new(&unsupported, &specs, NestedNulls::Reject)
            .err()
            .assured("null is unsupported for JSON columns");
        assert!(matches!(
            unsupported.current_context(),
            JsonColumnError::Unsupported { .. }
        ));
    }
}
