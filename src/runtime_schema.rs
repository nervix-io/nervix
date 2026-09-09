//! Schemas and codecs: the boundary between an external encoding and an Arrow batch.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Compiled schemas, compiled codecs, the Protobuf descriptor pool, decoding wire
//!   payloads directly into typed Arrow builders, encoding column values back out, and `RuntimeRow`
//!   — the shared view of one row in an Arc'd batch, which is the only in-memory representation of
//!   an individual payload.
//! - **Depends on.** The vocabulary, the jaq programs, and Arrow.
//! - **Must not know.** Relays, branches, schedules or the registry. A codec converts a payload and
//!   answers; it decides nothing about where the result goes.

use std::{borrow::Cow, io::Cursor, num::NonZeroU32, sync::Arc as StdArc};

use ahash::{HashMap, HashSet};
use apache_avro::{
    Schema as AvroSchema, from_avro_datum, to_avro_datum, types::Value as AvroValue,
};
use arch_into::ArchInto as _;
use arrow_array::{
    Array, ArrayRef, BooleanArray, FixedSizeListArray, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, ListArray, RecordBatch, RecordBatchOptions, StringArray,
    TimestampNanosecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    builder::{
        ArrayBuilder, BooleanBuilder, FixedSizeListBuilder, Float32Builder, Float64Builder,
        Int8Builder, Int16Builder, Int32Builder, Int64Builder, ListBuilder, StringBuilder,
        TimestampNanosecondBuilder, UInt8Builder, UInt16Builder, UInt32Builder, UInt64Builder,
        make_builder,
    },
    make_array, new_empty_array,
};
use arrow_data::transform::MutableArrayData;
use arrow_schema::{
    DataType as ArrowDataType, Field as ArrowField, FieldRef as ArrowFieldRef,
    Schema as ArrowSchema, TimeUnit as ArrowTimeUnit,
};
use arrow_select::{
    concat::concat as concat_arrow_arrays,
    filter::{filter as filter_arrow_array, filter_record_batch},
    take::take,
};
use chrono::{DateTime, FixedOffset};
use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_approx_into::ApproxInto;
use nervix_models::{
    AvroType, CodecJaqTransformations, CreateCodec, CreateSchema, CreateWireSchema, JsonType,
    ModelName, ParseAsType, RemoteRuntimeElementValue, RemoteRuntimeField, RemoteRuntimeRecord,
    RemoteRuntimeRecordMetadata, RemoteRuntimeValue, ResolvedCodecWireFormat, Timestamp,
    WireSchemaField, WireSchemaStrictness,
};
use nervix_wasm::{WasmProcessorField, WasmProcessorSchema, WasmProcessorType};
use ordered_float::OrderedFloat;
use prost::Message as ProstMessage;
use prost_reflect::{
    DescriptorPool, DeserializeOptions as ProtobufDeserializeOptions, DynamicMessage,
    MessageDescriptor, SerializeOptions as ProtobufSerializeOptions,
};
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    ser::{SerializeMap, SerializeSeq},
};
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};
use thiserror::Error;
use triomphe::Arc;

use crate::jaq_program::{CompiledJaqProgram, JaqNativeFormat};

mod arrow_body;
mod syslog;

#[derive(Debug, Clone)]
pub struct CompiledSchema {
    fields: Vec<CompiledSchemaField>,
    arrow_schema: StdArc<ArrowSchema>,
}

#[derive(Debug, Clone)]
pub(crate) struct CompiledSchemaField {
    name: String,
    ty: ParseAsType,
    optional: bool,
    sensitive: bool,
}

#[derive(Debug, Clone)]
pub struct CompiledCodec {
    pub name: ModelName,
    schema: Arc<CompiledSchema>,
    wire_schema: CompiledWireSchema,
}

pub(crate) struct CompiledCodecBatchEncoder<'a> {
    codec: &'a CompiledCodec,
    batch: &'a RuntimeRecordBatch,
}

#[derive(Debug, Clone)]
enum CompiledWireSchema {
    Json(CompiledJsonWireSchema),
    Cbor(CompiledJsonWireSchema),
    Avro(CompiledAvroWireSchema),
    JaqNative(CompiledJaqNativeCodec),
    Protobuf(CompiledProtobufCodec),
    Syslog,
}

#[derive(Debug, Clone)]
struct CompiledJsonWireSchema {
    strictness: WireSchemaStrictness,
    fields: HashMap<String, CompiledJsonWireField>,
}

#[derive(Debug, Clone)]
struct CompiledAvroWireSchema {
    fields: HashMap<String, CompiledAvroWireField>,
    schema: AvroSchema,
}

#[derive(Debug, Clone)]
struct CompiledJaqNativeCodec {
    format: JaqNativeFormat,
    transformations: CompiledJaqTransformations,
}

#[derive(Debug, Clone, Default)]
struct CompiledJaqTransformations {
    on_ingestion: Option<Arc<CompiledJaqProgram>>,
    on_emitting: Option<Arc<CompiledJaqProgram>>,
}

impl CompiledJaqTransformations {
    fn compile(
        codec: &CreateCodec,
        transformations: &CodecJaqTransformations,
    ) -> Result<Self, CodecError> {
        let compile = |program: Option<&str>| {
            program
                .map(|program| {
                    CompiledJaqProgram::compile(program)
                        .map(Arc::new)
                        .map_err(|error| CodecError::InvalidJaqTransformation {
                            codec: codec.name.as_str().to_string(),
                            reason: error.to_string(),
                        })
                })
                .transpose()
        };
        Ok(Self {
            on_ingestion: compile(transformations.on_ingestion.as_deref())?,
            on_emitting: compile(transformations.on_emitting.as_deref())?,
        })
    }
}

#[derive(Debug, Clone)]
struct CompiledProtobufCodec {
    message: MessageDescriptor,
    transformations: CompiledJaqTransformations,
}

/// A protobuf descriptor pool compiled from a resource version.
#[derive(Debug, Clone)]
pub struct ProtobufDescriptorPool {
    pool: DescriptorPool,
}

impl ProtobufDescriptorPool {
    pub fn from_file_descriptor_set(
        file_descriptor_set: prost_types::FileDescriptorSet,
    ) -> Result<Self, String> {
        DescriptorPool::from_file_descriptor_set(file_descriptor_set)
            .map(|pool| Self { pool })
            .map_err(|source| format!("invalid protobuf descriptor set: {source}"))
    }

    pub fn message(&self, message_name: &str) -> Result<MessageDescriptor, String> {
        self.pool
            .get_message_by_name(message_name)
            .ok_or_else(|| format!("protobuf message '{message_name}' was not found"))
    }
}

#[derive(Debug, Clone, Copy)]
struct CompiledJsonWireField {
    ty: JsonType,
    optional: bool,
}

#[derive(Debug, Clone, Copy)]
struct CompiledAvroWireField {
    ty: AvroType,
    optional: bool,
    position: usize,
}

/// One relay payload: an Arrow batch and nothing beside it.
///
/// The batch carries its own schema, so there is no second description of these columns that
/// could disagree with them. [`RuntimeRecordBatch::from_record_batch`] is where a batch decoded
/// from outside is checked against the schema its relay expects.
#[derive(Debug, Clone)]
pub struct RuntimeRecordBatch {
    batch: RecordBatch,
}

/// The Arrow columns one batch is built into, one row at a time.
///
/// A caller opens one builder per batch and appends every row into it, so a batch of `n` rows
/// costs one set of columns rather than `n` sets and a concatenation. A row that fails part-way
/// through is closed by [`RuntimeRecordBatchBuilder::abandon_row`] and dropped at `finish`,
/// because an Arrow builder cannot give a value back.
pub(crate) struct RuntimeRecordBatchBuilder {
    schema: StdArc<ArrowSchema>,
    fields: Vec<CompiledSchemaField>,
    builders: Vec<Box<dyn ArrayBuilder>>,
    /// One entry per appended row, `false` for a row the batch drops when it is finished.
    keep: Vec<bool>,
    /// How many of `keep` are `false`, so the row count stays a read rather than a scan.
    abandoned: usize,
    next_column: usize,
}

// Counted per thread so a test observes only the batches it built itself, while the rest of the
// suite exercises the same builders in parallel.
#[cfg(test)]
thread_local! {
    pub(crate) static RECORD_BUILDER_SETS_OPENED: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    pub(crate) static RECORD_COLUMN_SETS_BUILT: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}

/// A shared view of one row in an Arrow payload batch.
///
/// This is the only in-memory representation of an individual schemaful payload. It keeps the
/// carrier columns shared and materializes only explicitly requested scalar boundary values.
#[derive(Debug, Clone)]
pub struct RuntimeRow {
    batch: Arc<RuntimeRecordBatch>,
    row: usize,
    metadata: RuntimeRecordMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeRecordMetadata {
    ingested_at_low_watermark: Timestamp,
    ingested_at_high_watermark: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RuntimeValue {
    U8(u8),
    I8(i8),
    U16(u16),
    I16(i16),
    U32(u32),
    I32(i32),
    U64(u64),
    I64(i64),
    Bool(bool),
    String(String),
    Datetime(DateTime<FixedOffset>),
    F32(OrderedFloat<f32>),
    F64(OrderedFloat<f64>),
    Array(Vec<RuntimeValue>),
    Vec(Vec<RuntimeValue>),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
enum SerializableRuntimeValue {
    U8(u8),
    I8(i8),
    U16(u16),
    I16(i16),
    U32(u32),
    I32(i32),
    U64(u64),
    I64(i64),
    Bool(bool),
    String(String),
    Datetime(String),
    F32(f32),
    F64(f64),
    Array(Vec<SerializableRuntimeValue>),
    Vec(Vec<SerializableRuntimeValue>),
}

#[derive(Debug, Error)]
pub enum CodecError {
    #[error("codec '{codec}' is incompatible: {reason}")]
    InvalidCodec { codec: String, reason: String },
    #[error("failed to parse json payload for codec '{codec}': {source}")]
    JsonDecode {
        codec: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("failed to encode json payload for codec '{codec}': {source}")]
    JsonEncode {
        codec: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("failed to parse json payload for codec '{codec}': {source}")]
    SimdJsonDecode {
        codec: String,
        #[source]
        source: simd_json::Error,
    },
    #[error("failed to encode json payload for codec '{codec}': {source}")]
    SimdJsonEncode {
        codec: String,
        #[source]
        source: simd_json::Error,
    },
    #[error("failed to parse cbor payload for codec '{codec}': {reason}")]
    CborDecode { codec: String, reason: String },
    #[error("failed to encode cbor payload for codec '{codec}': {reason}")]
    CborEncode { codec: String, reason: String },
    #[error("failed to parse avro payload for codec '{codec}': {source}")]
    AvroDecode {
        codec: String,
        #[source]
        source: apache_avro::Error,
    },
    #[error("failed to encode avro payload for codec '{codec}': {source}")]
    AvroEncode {
        codec: String,
        #[source]
        source: apache_avro::Error,
    },
    #[error("failed to parse {format} payload for codec '{codec}': {reason}")]
    JaqNativeDecode {
        codec: String,
        format: &'static str,
        reason: String,
    },
    #[error("failed to encode {format} payload for codec '{codec}': {reason}")]
    JaqNativeEncode {
        codec: String,
        format: &'static str,
        reason: String,
    },
    #[error("failed to parse protobuf payload for codec '{codec}': {reason}")]
    ProtobufDecode { codec: String, reason: String },
    #[error("failed to encode protobuf payload for codec '{codec}': {reason}")]
    ProtobufEncode { codec: String, reason: String },
    #[error("failed to parse syslog payload for codec '{codec}': {reason}")]
    SyslogDecode { codec: String, reason: String },
    #[error("failed to encode syslog payload for codec '{codec}': {reason}")]
    SyslogEncode { codec: String, reason: String },
    #[error("codec '{codec}' expected object payload")]
    ExpectedObject { codec: String },
    #[error("codec '{codec}' has invalid jaq transformation: {reason}")]
    InvalidJaqTransformation { codec: String, reason: String },
    #[error("codec '{codec}' jaq transformation failed: {reason}")]
    JaqTransform { codec: String, reason: String },
    #[error("codec '{codec}' missing field '{field}'")]
    MissingField { codec: String, field: String },
    #[error("codec '{codec}' has unexpected field '{field}'")]
    UnexpectedField { codec: String, field: String },
    #[error("codec '{codec}' failed to parse field '{field}': {reason}")]
    ParseField {
        codec: String,
        field: String,
        reason: String,
    },
    #[error("codec '{codec}' failed to encode field '{field}': {reason}")]
    EncodeField {
        codec: String,
        field: String,
        reason: String,
    },
}

impl CompiledSchema {
    pub(crate) fn fields(&self) -> &[CompiledSchemaField] {
        &self.fields
    }

    pub fn arrow_schema(&self) -> StdArc<ArrowSchema> {
        self.arrow_schema.clone()
    }

    pub(crate) fn vm_sensitivity(&self) -> nervix_vm::SchemaSensitivity {
        nervix_vm::SchemaSensitivity::from_sensitive_fields(
            self.fields
                .iter()
                .filter(|field| field.sensitive)
                .map(|field| field.name.clone()),
        )
    }

    pub(crate) fn wasm_processor_schema(&self, name: impl Into<String>) -> WasmProcessorSchema {
        WasmProcessorSchema {
            name: name.into(),
            fields: self
                .fields
                .iter()
                .map(|field| WasmProcessorField {
                    name: field.name.clone(),
                    ty: WasmProcessorType::from(&field.ty),
                    optional: field.optional,
                })
                .collect(),
        }
    }

    /// Opens one builder for a batch of `capacity` rows.
    pub(crate) fn batch_builder(&self, capacity: usize) -> RuntimeRecordBatchBuilder {
        #[cfg(test)]
        RECORD_BUILDER_SETS_OPENED.with(|count| count.set(count.get() + 1));
        RuntimeRecordBatchBuilder {
            schema: self.arrow_schema.clone(),
            fields: self.fields.clone(),
            builders: self
                .fields
                .iter()
                .map(|field| make_builder(&arrow_data_type(&field.ty), capacity))
                .collect(),
            keep: Vec::with_capacity(capacity),
            abandoned: 0,
            next_column: 0,
        }
    }

    #[cfg(test)]
    pub(crate) fn batch_from_test_rows(
        &self,
        rows: impl IntoIterator<Item = impl IntoIterator<Item = (String, RuntimeValue)>>,
    ) -> Result<RuntimeRecordBatch, String> {
        let rows = rows
            .into_iter()
            .map(|row| row.into_iter().collect::<Vec<_>>())
            .collect::<Vec<_>>();
        let mut builder = self.batch_builder(rows.len());
        for (row_index, row) in rows.iter().enumerate() {
            for (field_index, (name, _)) in row.iter().enumerate() {
                if row[..field_index]
                    .iter()
                    .any(|(earlier_name, _)| earlier_name == name)
                {
                    return Err(format!(
                        "test Arrow row {row_index} contains duplicate field '{name}'"
                    ));
                }
                if !self.fields.iter().any(|field| field.name == *name) {
                    return Err(format!(
                        "test Arrow row {row_index} contains unknown field '{name}'"
                    ));
                }
            }
            for field in &self.fields {
                builder.append(
                    row.iter()
                        .find(|(name, _)| *name == field.name)
                        .map(|(_, value)| value),
                )?;
            }
            builder.finish_row()?;
        }
        builder.finish()
    }

    pub(crate) fn runtime_row_from_remote(
        &self,
        record: RemoteRuntimeRecord,
    ) -> Result<RuntimeRow, String> {
        let (batch, metadata) = self.runtime_batch_from_remote(record)?;
        batch.runtime_row(0, metadata)
    }

    pub(crate) fn runtime_batch_from_remote(
        &self,
        record: RemoteRuntimeRecord,
    ) -> Result<(RuntimeRecordBatch, RuntimeRecordMetadata), String> {
        let metadata = RuntimeRecordMetadata::from_remote(record.metadata);
        let mut seen = HashSet::default();
        for field in &record.fields {
            if !seen.insert(field.name.as_str()) {
                return Err(format!(
                    "persisted runtime record contains duplicate field '{}'",
                    field.name
                ));
            }
            if !self
                .fields
                .iter()
                .any(|expected| expected.name == field.name)
            {
                return Err(format!(
                    "persisted runtime record contains unknown field '{}'",
                    field.name
                ));
            }
        }
        let mut builder = self.batch_builder(1);
        for expected in &self.fields {
            let value = record
                .fields
                .iter()
                .find(|field| field.name == expected.name)
                .map(|field| RuntimeValue::from_remote(field.value.clone()));
            builder.append(value.as_ref())?;
        }
        builder.finish_row()?;
        builder.finish().map(|batch| (batch, metadata))
    }

    fn validate_arrow_batch(&self, batch: &RuntimeRecordBatch) -> Result<(), String> {
        if batch.schema_ref().as_ref() != self.arrow_schema.as_ref() {
            return Err("arrow batch schema does not match compiled schema".to_string());
        }
        if batch.batch.num_columns() != self.fields.len() {
            return Err(format!(
                "arrow batch column count {} does not match schema field count {}",
                batch.batch.num_columns(),
                self.fields.len()
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
pub(crate) fn test_runtime_row(
    fields: impl IntoIterator<Item = (String, RuntimeValue)>,
) -> RuntimeRow {
    let fields = fields.into_iter().collect::<Vec<_>>();
    for (field_index, (name, _)) in fields.iter().enumerate() {
        assert!(
            !fields[..field_index]
                .iter()
                .any(|(earlier_name, _)| earlier_name == name),
            "test Arrow row contains duplicate field '{name}'"
        );
    }
    let arrow_fields = fields
        .iter()
        .map(|(name, value)| {
            test_runtime_value_arrow_type(value)
                .map(|data_type| ArrowField::new(name, data_type, false))
        })
        .collect::<Result<Vec<_>, _>>()
        .verified("the columns are built from the very types inferred from these same values");
    let schema = StdArc::new(ArrowSchema::new(arrow_fields));
    let columns = fields
        .iter()
        .zip(schema.fields())
        .map(|((_, value), field)| runtime_value_arrow_array(field.data_type(), Some(value), 1))
        .collect::<Result<Vec<_>, _>>()
        .verified("the columns are built from the very types inferred from these same values");
    let batch = if columns.is_empty() {
        RecordBatch::try_new_with_options(
            schema.clone(),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(1)),
        )
    } else {
        RecordBatch::try_new(schema.clone(), columns)
    }
    .verified("the columns are built from the very types inferred from these same values");
    RuntimeRecordBatch::from_record_batch(schema, batch)
        .and_then(|batch| batch.runtime_row(0, RuntimeRecordMetadata::test()))
        .verified("the columns are built from the very types inferred from these same values")
}

#[cfg(test)]
fn test_runtime_value_arrow_type(value: &RuntimeValue) -> Result<ArrowDataType, String> {
    match value {
        RuntimeValue::U8(_) => Ok(ArrowDataType::UInt8),
        RuntimeValue::I8(_) => Ok(ArrowDataType::Int8),
        RuntimeValue::U16(_) => Ok(ArrowDataType::UInt16),
        RuntimeValue::I16(_) => Ok(ArrowDataType::Int16),
        RuntimeValue::U32(_) => Ok(ArrowDataType::UInt32),
        RuntimeValue::I32(_) => Ok(ArrowDataType::Int32),
        RuntimeValue::U64(_) => Ok(ArrowDataType::UInt64),
        RuntimeValue::I64(_) => Ok(ArrowDataType::Int64),
        RuntimeValue::Bool(_) => Ok(ArrowDataType::Boolean),
        RuntimeValue::String(_) => Ok(ArrowDataType::Utf8),
        RuntimeValue::Datetime(_) => Ok(ArrowDataType::Timestamp(
            ArrowTimeUnit::Nanosecond,
            Some("+00:00".into()),
        )),
        RuntimeValue::F32(_) => Ok(ArrowDataType::Float32),
        RuntimeValue::F64(_) => Ok(ArrowDataType::Float64),
        RuntimeValue::Array(values) => {
            let element = values
                .first()
                .ok_or_else(|| "cannot infer an empty test ARRAY element type".to_string())?;
            Ok(ArrowDataType::FixedSizeList(
                ArrowFieldRef::new(ArrowField::new(
                    "item",
                    test_runtime_value_arrow_type(element)?,
                    false,
                )),
                i32::try_from(values.len())
                    .map_err(|_| "test ARRAY length exceeds i32".to_string())?,
            ))
        }
        RuntimeValue::Vec(values) => {
            let element = values
                .first()
                .ok_or_else(|| "cannot infer an empty test VEC element type".to_string())?;
            Ok(ArrowDataType::List(ArrowFieldRef::new(ArrowField::new(
                "item",
                test_runtime_value_arrow_type(element)?,
                false,
            ))))
        }
    }
}

impl CompiledCodec {
    pub(crate) fn schema(&self) -> Arc<CompiledSchema> {
        self.schema.clone()
    }

    /// Runs the ON INGESTION transformation that [`Self::requires_blocking_decode`] selects,
    /// without touching a single Arrow column.
    ///
    /// jaq and protobuf decoding is the CPU-bound half of those codecs, and it produces a JSON
    /// value before any column is written. Naming that half separately is what lets a caller run
    /// it off the reactor and then [`Self::append_transformed_row`] on the task that owns the
    /// batch builder, instead of sending the builder to another thread.
    pub(crate) fn transform_on_ingestion(&self, payload: &[u8]) -> Result<JsonValue, CodecError> {
        match &self.wire_schema {
            CompiledWireSchema::JaqNative(native) => transform_jaq_native(self, native, payload),
            CompiledWireSchema::Protobuf(protobuf) => transform_protobuf(self, protobuf, payload),
            CompiledWireSchema::Json(_)
            | CompiledWireSchema::Cbor(_)
            | CompiledWireSchema::Avro(_)
            | CompiledWireSchema::Syslog => Err(CodecError::InvalidCodec {
                codec: self.name.as_str().to_string(),
                reason: "codec declares no ON INGESTION transformation to run".to_string(),
            }),
        }
    }

    /// Appends the result of [`Self::transform_on_ingestion`] as one row of `builder`.
    pub(crate) fn append_transformed_row(
        &self,
        value: &JsonValue,
        builder: &mut RuntimeRecordBatchBuilder,
    ) -> Result<(), CodecError> {
        let appended = decode_json_value(self, value, None, builder);
        finish_decoded_row(self, builder, appended)
    }

    pub fn requires_blocking_decode(&self) -> bool {
        match &self.wire_schema {
            CompiledWireSchema::JaqNative(native) => native.transformations.on_ingestion.is_some(),
            CompiledWireSchema::Protobuf(protobuf) => {
                protobuf.transformations.on_ingestion.is_some()
            }
            CompiledWireSchema::Json(_)
            | CompiledWireSchema::Cbor(_)
            | CompiledWireSchema::Avro(_)
            | CompiledWireSchema::Syslog => false,
        }
    }

    pub(crate) fn requires_blocking_encode(&self) -> bool {
        match &self.wire_schema {
            CompiledWireSchema::JaqNative(native) => native.transformations.on_emitting.is_some(),
            CompiledWireSchema::Protobuf(protobuf) => {
                protobuf.transformations.on_emitting.is_some()
            }
            CompiledWireSchema::Json(_)
            | CompiledWireSchema::Cbor(_)
            | CompiledWireSchema::Avro(_)
            | CompiledWireSchema::Syslog => false,
        }
    }

    pub(crate) fn batch_encoder<'a>(
        &'a self,
        batch: &'a RuntimeRecordBatch,
    ) -> Result<CompiledCodecBatchEncoder<'a>, CodecError> {
        self.schema
            .validate_arrow_batch(batch)
            .map_err(|reason| CodecError::InvalidCodec {
                codec: self.name.as_str().to_string(),
                reason,
            })?;
        Ok(CompiledCodecBatchEncoder { codec: self, batch })
    }
}

impl CompiledCodecBatchEncoder<'_> {
    pub(crate) fn encode_row_into(
        &self,
        row_index: usize,
        payload: &mut Vec<u8>,
    ) -> Result<(), CodecError> {
        if row_index >= self.batch.batch.num_rows() {
            return Err(CodecError::InvalidCodec {
                codec: self.codec.name.as_str().to_string(),
                reason: format!(
                    "columnar encode row {row_index} is outside batch with {} rows",
                    self.batch.batch.num_rows()
                ),
            });
        }
        payload.clear();
        let row = ArrowCodecRow::new(self.codec, self.batch, row_index);
        match &self.codec.wire_schema {
            CompiledWireSchema::Json(_) => {
                simd_json::to_writer(&mut *payload, &row).map_err(|source| {
                    CodecError::SimdJsonEncode {
                        codec: self.codec.name.as_str().to_string(),
                        source,
                    }
                })?;
            }
            CompiledWireSchema::Cbor(_) => {
                ciborium::into_writer(&row, &mut *payload).map_err(|source| {
                    CodecError::CborEncode {
                        codec: self.codec.name.as_str().to_string(),
                        reason: source.to_string(),
                    }
                })?;
            }
            CompiledWireSchema::Avro(wire_schema) => {
                let value = row.to_avro_record(wire_schema)?;
                *payload = to_avro_datum(&wire_schema.schema, value).map_err(|source| {
                    CodecError::AvroEncode {
                        codec: self.codec.name.as_str().to_string(),
                        source,
                    }
                })?;
            }
            CompiledWireSchema::JaqNative(native) => {
                let Some(program) = native.transformations.on_emitting.as_deref() else {
                    return Err(CodecError::InvalidCodec {
                        codec: self.codec.name.as_str().to_string(),
                        reason: "JAQ-native codec used for encoding must declare ON EMITTING \
                                 transformation"
                            .to_string(),
                    });
                };
                let value = run_jaq_transformation(self.codec, program, row.to_json_value()?)?;
                *payload = native.format.write_value(value).map_err(|error| {
                    CodecError::JaqNativeEncode {
                        codec: self.codec.name.as_str().to_string(),
                        format: native.format.name(),
                        reason: error.to_string(),
                    }
                })?;
            }
            CompiledWireSchema::Protobuf(protobuf) => {
                let Some(program) = protobuf.transformations.on_emitting.as_deref() else {
                    return Err(CodecError::InvalidCodec {
                        codec: self.codec.name.as_str().to_string(),
                        reason: "protobuf codec used for encoding must declare ON EMITTING \
                                 transformation"
                            .to_string(),
                    });
                };
                let value = run_jaq_transformation(self.codec, program, row.to_json_value()?)?;
                *payload =
                    encode_protobuf_payload(&protobuf.message, &value).map_err(|reason| {
                        CodecError::ProtobufEncode {
                            codec: self.codec.name.as_str().to_string(),
                            reason,
                        }
                    })?;
            }
            CompiledWireSchema::Syslog => syslog::encode_row(&row, payload)?,
        }
        Ok(())
    }
}

/// The bytes a batch's columns actually hold.
///
/// Every relay size limit is written in these terms: `MAX BATCH SIZE`, the relay metrics, and the
/// decoded bound a peer's body is held to. Arrow's `get_array_memory_size` reports the capacity a
/// decoder allocated instead, which for a string column runs many times the data it carries, so a
/// limit expressed in payload bytes must never be enforced with it.
pub(crate) fn batch_payload_bytes(batch: &RecordBatch) -> u64 {
    batch
        .columns()
        .iter()
        .map(|column| {
            let Ok(bytes) = column.to_data().get_slice_memory_size() else {
                return u64::MAX;
            };
            bytes.arch_into()
        })
        .fold(0_u64, u64::saturating_add)
}

impl RuntimeRecordBatch {
    pub(crate) fn from_record_batch(
        expected_schema: StdArc<ArrowSchema>,
        batch: RecordBatch,
    ) -> Result<Self, String> {
        if batch.schema().as_ref() != expected_schema.as_ref() {
            return Err("arrow batch schema does not match expected schema".to_string());
        }
        Ok(Self { batch })
    }

    pub fn schema(&self) -> StdArc<ArrowSchema> {
        StdArc::clone(self.batch.schema_ref())
    }

    fn schema_ref(&self) -> &StdArc<ArrowSchema> {
        self.batch.schema_ref()
    }

    pub fn batch(&self) -> &RecordBatch {
        &self.batch
    }

    pub(crate) fn estimated_bytes(&self) -> u64 {
        batch_payload_bytes(&self.batch)
    }

    pub(crate) fn from_rows<'a>(
        expected_schema: StdArc<ArrowSchema>,
        rows: impl ExactSizeIterator<Item = &'a RuntimeRow>,
    ) -> Result<Self, String> {
        let row_count = rows.len();
        if row_count == 0 {
            let columns = expected_schema
                .fields()
                .iter()
                .map(|field| new_empty_array(field.data_type()))
                .collect::<Vec<_>>();
            let batch = RecordBatch::try_new(expected_schema.clone(), columns)
                .map_err(|error| error.to_string())?;
            return Ok(Self { batch });
        }

        let mut source_indices = HashMap::default();
        let mut sources = Vec::new();
        let mut selections = Vec::with_capacity(row_count);
        for row in rows {
            if row.batch.schema_ref().as_ref() != expected_schema.as_ref() {
                return Err("Arrow row schema does not match expected schema".to_string());
            }
            if row.row >= row.batch.batch.num_rows() {
                return Err(format!(
                    "Arrow batch row {} is outside batch with {} rows",
                    row.row,
                    row.batch.batch.num_rows()
                ));
            }
            let pointer = Arc::as_ptr(&row.batch);
            let source = if let Some(source) = source_indices.get(&pointer) {
                *source
            } else {
                let source = sources.len();
                sources.push(row.batch.as_ref());
                source_indices.insert(pointer, source);
                source
            };
            selections.push((source, row.row));
        }

        if sources.len() == 1 {
            let start = selections[0].1;
            if selections
                .iter()
                .enumerate()
                .all(|(offset, (_, row))| Some(*row) == start.checked_add(offset))
            {
                if start == 0 && row_count == sources[0].batch.num_rows() {
                    return Ok(sources[0].clone());
                }
                return sources[0].slice(start, row_count);
            }
        }

        let columns = (0..expected_schema.fields().len())
            .map(|column| {
                let source_data = sources
                    .iter()
                    .map(|source| source.batch.column(column).to_data())
                    .collect::<Vec<_>>();
                let mut output =
                    MutableArrayData::new(source_data.iter().collect(), false, row_count);
                for (source, row) in &selections {
                    let end = row
                        .checked_add(1)
                        .verified("the selection names a row of a batch held in memory");
                    output.extend(*source, *row, end);
                }
                make_array(output.freeze())
            })
            .collect::<Vec<_>>();
        let batch = if columns.is_empty() {
            RecordBatch::try_new_with_options(
                expected_schema.clone(),
                columns,
                &RecordBatchOptions::new().with_row_count(Some(row_count)),
            )
        } else {
            RecordBatch::try_new(expected_schema.clone(), columns)
        }
        .map_err(|error| error.to_string())?;
        Ok(Self { batch })
    }

    pub(crate) fn shared_from_rows(
        expected_schema: StdArc<ArrowSchema>,
        rows: &[RuntimeRow],
    ) -> Result<Arc<Self>, String> {
        if let Some(first) = rows.first()
            && first.batch.schema_ref().as_ref() == expected_schema.as_ref()
            && rows.len() == first.batch.batch.num_rows()
            && rows
                .iter()
                .enumerate()
                .all(|(index, row)| Arc::ptr_eq(&first.batch, &row.batch) && row.row == index)
        {
            return Ok(first.batch.clone());
        }
        Self::from_rows(expected_schema, rows.iter()).map(Arc::new)
    }

    pub(crate) fn value(&self, row: usize, name: &str) -> Result<Option<RuntimeValue>, String> {
        if row >= self.batch.num_rows() {
            return Err(format!(
                "Arrow batch row {row} is outside batch with {} rows",
                self.batch.num_rows()
            ));
        }
        let column_index = match self.schema_ref().index_of(name) {
            Ok(index) => index,
            Err(_) => return Ok(None),
        };
        self.value_at(row, column_index)
    }

    pub(crate) fn value_at(
        &self,
        row: usize,
        column_index: usize,
    ) -> Result<Option<RuntimeValue>, String> {
        if row >= self.batch.num_rows() {
            return Err(format!(
                "Arrow batch row {row} is outside batch with {} rows",
                self.batch.num_rows()
            ));
        }
        let field = self
            .schema_ref()
            .fields()
            .get(column_index)
            .ok_or_else(|| {
                format!(
                    "Arrow column index {column_index} is outside schema with {} fields",
                    self.schema_ref().fields().len()
                )
            })?;
        let column = self.batch.columns().get(column_index).ok_or_else(|| {
            format!(
                "Arrow column index {column_index} is outside batch with {} columns",
                self.batch.num_columns()
            )
        })?;
        runtime_value_from_arrow_array(
            column.as_ref(),
            &parse_as_type_from_arrow(field.data_type()).map_err(|error| error.to_string())?,
            field.is_nullable(),
            row,
            field.name(),
        )
    }

    pub(crate) fn row_to_json_string(&self, row: usize) -> Result<String, String> {
        self.row_to_json_string_masking(row, &nervix_vm::SchemaSensitivity::default())
    }

    fn row_to_json_string_masking(
        &self,
        row: usize,
        sensitivity: &nervix_vm::SchemaSensitivity,
    ) -> Result<String, String> {
        if row >= self.batch.num_rows() {
            return Err(format!(
                "Arrow batch row {row} is outside batch with {} rows",
                self.batch.num_rows()
            ));
        }
        let mut json = JsonMap::new();
        let mut fields = self.schema_ref().fields().iter().collect::<Vec<_>>();
        fields.sort_by(|left, right| left.name().cmp(right.name()));
        for field in fields {
            if let Some(value) = self.value(row, field.name())? {
                let value = if sensitivity.is_sensitive(field.name()) {
                    JsonValue::String("<masked>".to_string())
                } else {
                    value.to_json_value()
                };
                json.insert(field.name().clone(), value);
            }
        }
        Ok(JsonValue::Object(json).to_string())
    }

    pub(crate) fn slice(&self, offset: usize, length: usize) -> Result<Self, String> {
        let end = offset.checked_add(length).ok_or_else(|| {
            format!("Arrow batch slice offset {offset} and length {length} overflow")
        })?;
        if end > self.batch.num_rows() {
            return Err(format!(
                "Arrow batch slice {offset}..{end} is outside batch with {} rows",
                self.batch.num_rows()
            ));
        }
        Ok(Self {
            batch: self.batch.slice(offset, length),
        })
    }

    pub(crate) fn take(&self, rows: &[usize]) -> Result<Self, String> {
        let indices = UInt64Array::from(
            rows.iter()
                .map(|row| {
                    if *row >= self.batch.num_rows() {
                        return Err(format!(
                            "Arrow batch row {row} is outside batch with {} rows",
                            self.batch.num_rows()
                        ));
                    }
                    Ok::<u64, String>((*row).arch_into())
                })
                .collect::<Result<Vec<_>, String>>()?,
        );
        let columns = self
            .batch
            .columns()
            .iter()
            .map(|column| take(column.as_ref(), &indices, None).map_err(|error| error.to_string()))
            .collect::<Result<Vec<_>, _>>()?;
        let batch = if columns.is_empty() {
            RecordBatch::try_new_with_options(
                self.schema(),
                columns,
                &RecordBatchOptions::new().with_row_count(Some(rows.len())),
            )
        } else {
            RecordBatch::try_new(self.schema(), columns)
        }
        .map_err(|error| error.to_string())?;
        Ok(Self { batch })
    }

    pub(crate) fn runtime_row(
        &self,
        row: usize,
        metadata: RuntimeRecordMetadata,
    ) -> Result<RuntimeRow, String> {
        RuntimeRow::new(Arc::new(self.clone()), row, metadata)
    }

    pub(crate) fn project(&self, schema: StdArc<ArrowSchema>) -> Result<Self, String> {
        let columns = schema
            .fields()
            .iter()
            .map(|field| {
                let index = self
                    .schema_ref()
                    .index_of(field.name())
                    .map_err(|_| format!("Arrow payload is missing field '{}'", field.name()))?;
                let column = self.batch.column(index);
                if column.data_type() != field.data_type() {
                    return Err(format!(
                        "Arrow payload field '{}' expected {:?}, found {:?}",
                        field.name(),
                        field.data_type(),
                        column.data_type()
                    ));
                }
                if !field.is_nullable() && column.null_count() > 0 {
                    return Err(format!(
                        "required Arrow payload field '{}' contains null values",
                        field.name()
                    ));
                }
                Ok(column.clone())
            })
            .collect::<Result<Vec<_>, String>>()?;
        let batch = if columns.is_empty() {
            RecordBatch::try_new_with_options(
                schema.clone(),
                columns,
                &RecordBatchOptions::new().with_row_count(Some(self.batch.num_rows())),
            )
        } else {
            RecordBatch::try_new(schema.clone(), columns)
        }
        .map_err(|error| error.to_string())?;
        Ok(Self { batch })
    }

    pub(crate) fn filter(&self, predicate: &BooleanArray) -> Result<Self, String> {
        if predicate.len() != self.batch.num_rows() {
            return Err(format!(
                "arrow filter predicate row count {} does not match batch row count {}",
                predicate.len(),
                self.batch.num_rows()
            ));
        }
        let batch =
            filter_record_batch(&self.batch, predicate).map_err(|error| error.to_string())?;
        Ok(Self { batch })
    }

    pub fn concat(batches: &[&Self]) -> Result<Self, String> {
        let Some(first) = batches.first() else {
            return Err("cannot concat zero arrow batches".to_string());
        };

        let schema = first.schema();
        if batches
            .iter()
            .any(|batch| batch.schema_ref().as_ref() != schema.as_ref())
        {
            return Err("cannot concat arrow batches with different schemas".to_string());
        }

        if batches.len() == 1 {
            return Ok((*first).clone());
        }

        let columns = if schema.fields().is_empty() {
            Vec::new()
        } else {
            let mut columns = Vec::with_capacity(schema.fields().len());
            for column_index in 0..schema.fields().len() {
                let arrays = batches
                    .iter()
                    .map(|batch| batch.batch.column(column_index).as_ref())
                    .collect::<Vec<_>>();
                columns.push(concat_arrow_arrays(&arrays).map_err(|error| error.to_string())?);
            }
            columns
        };

        let row_count = batches
            .iter()
            .map(|batch| batch.batch.num_rows())
            .sum::<usize>();
        let batch = if columns.is_empty() {
            RecordBatch::try_new_with_options(
                schema.clone(),
                columns,
                &RecordBatchOptions::new().with_row_count(Some(row_count)),
            )
        } else {
            RecordBatch::try_new(schema.clone(), columns)
        }
        .map_err(|error| error.to_string())?;

        Ok(Self { batch })
    }
}

impl RuntimeRow {
    pub(crate) fn new(
        batch: Arc<RuntimeRecordBatch>,
        row: usize,
        metadata: RuntimeRecordMetadata,
    ) -> Result<Self, String> {
        if row >= batch.batch.num_rows() {
            return Err(format!(
                "Arrow batch row {row} is outside batch with {} rows",
                batch.batch.num_rows()
            ));
        }
        Ok(Self {
            batch,
            row,
            metadata,
        })
    }

    pub(crate) fn batch(&self) -> &Arc<RuntimeRecordBatch> {
        &self.batch
    }

    #[cfg(test)]
    pub(crate) fn arrow_schema(&self) -> StdArc<ArrowSchema> {
        self.batch.schema()
    }

    pub(crate) fn metadata(&self) -> &RuntimeRecordMetadata {
        &self.metadata
    }

    #[cfg(test)]
    pub(crate) fn with_metadata(mut self, metadata: RuntimeRecordMetadata) -> Self {
        self.metadata = metadata;
        self
    }

    #[cfg(test)]
    pub(crate) fn with_ingested_at_watermarks(mut self, watermark: Timestamp) -> Self {
        self.metadata = RuntimeRecordMetadata::from_ingested_at_watermarks(watermark, watermark);
        self
    }

    pub(crate) fn value(&self, name: &str) -> Result<Option<RuntimeValue>, String> {
        self.batch.value(self.row, name)
    }

    pub(crate) fn value_at(&self, column_index: usize) -> Result<Option<RuntimeValue>, String> {
        self.batch.value_at(self.row, column_index)
    }

    pub(crate) fn from_remote(
        schema: StdArc<ArrowSchema>,
        record: RemoteRuntimeRecord,
    ) -> Result<Self, String> {
        let metadata = RuntimeRecordMetadata::from_remote(record.metadata);
        let mut seen = HashSet::default();
        for field in &record.fields {
            if !seen.insert(field.name.as_str()) {
                return Err(format!(
                    "persisted runtime record contains duplicate field '{}'",
                    field.name
                ));
            }
            if schema.index_of(&field.name).is_err() {
                return Err(format!(
                    "persisted runtime record contains unknown field '{}'",
                    field.name
                ));
            }
        }
        let mut builder = RuntimeRecordBatchBuilder::from_arrow_schema(schema, 1)?;
        let expected_fields = builder.fields.clone();
        for expected in &expected_fields {
            let value = record
                .fields
                .iter()
                .find(|field| field.name == expected.name)
                .map(|field| RuntimeValue::from_remote(field.value.clone()));
            builder.append(value.as_ref())?;
        }
        builder.finish_row()?;
        builder.finish()?.runtime_row(0, metadata)
    }

    pub(crate) fn one_row_batch(&self) -> RuntimeRecordBatch {
        RuntimeRecordBatch {
            batch: self.batch.batch.slice(self.row, 1),
        }
    }

    #[cfg(test)]
    pub(crate) fn to_json_string(&self) -> Result<String, String> {
        self.to_json_string_masking(&nervix_vm::SchemaSensitivity::default())
    }

    pub(crate) fn to_json_string_masking(
        &self,
        sensitivity: &nervix_vm::SchemaSensitivity,
    ) -> Result<String, String> {
        self.batch.row_to_json_string_masking(self.row, sensitivity)
    }

    pub(crate) fn to_remote(&self) -> Result<RemoteRuntimeRecord, String> {
        let mut fields = Vec::with_capacity(self.batch.schema_ref().fields().len());
        for (column_index, field) in self.batch.schema_ref().fields().iter().enumerate() {
            if let Some(value) = self.value_at(column_index)? {
                fields.push(RemoteRuntimeField {
                    name: field.name().clone(),
                    value: value.to_remote(),
                });
            }
        }
        Ok(RemoteRuntimeRecord {
            fields,
            metadata: self.metadata.to_remote(),
        })
    }
}

pub(crate) fn remote_runtime_record_to_json_string(record: &RemoteRuntimeRecord) -> String {
    let mut fields = record.fields.iter().collect::<Vec<_>>();
    fields.sort_by(|left, right| left.name.cmp(&right.name));
    JsonValue::Object(
        fields
            .into_iter()
            .map(|field| {
                (
                    field.name.clone(),
                    RuntimeValue::from_remote(field.value.clone()).to_json_value(),
                )
            })
            .collect(),
    )
    .to_string()
}

impl RuntimeRecordBatchBuilder {
    fn from_arrow_schema(schema: StdArc<ArrowSchema>, capacity: usize) -> Result<Self, String> {
        let fields = schema
            .fields()
            .iter()
            .map(|field| {
                Ok(CompiledSchemaField {
                    name: field.name().clone(),
                    ty: parse_as_type_from_arrow(field.data_type())
                        .map_err(|error| error.to_string())?,
                    optional: field.is_nullable(),
                    sensitive: false,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        let builders = fields
            .iter()
            .map(|field| make_builder(&arrow_data_type(&field.ty), capacity))
            .collect();
        Ok(Self {
            schema,
            fields,
            builders,
            keep: Vec::with_capacity(capacity),
            abandoned: 0,
            next_column: 0,
        })
    }

    fn next_field_index(&self) -> Result<usize, String> {
        let index = self.next_column;
        self.fields.get(index).ok_or_else(|| {
            format!(
                "Arrow batch row {} already contains all {} schema fields",
                self.keep.len(),
                self.fields.len()
            )
        })?;
        Ok(index)
    }

    pub(crate) fn append(&mut self, value: Option<&RuntimeValue>) -> Result<(), String> {
        let index = self.next_field_index()?;
        let field = &self.fields[index];
        if value.is_none() && !field.optional {
            return Err(format!(
                "Arrow batch row {} is missing required field '{}'",
                self.keep.len(),
                field.name
            ));
        }
        append_runtime_value_to_arrow(
            self.builders[index].as_mut(),
            &field.ty,
            value,
            &format!("Arrow batch row {} field '{}'", self.keep.len(), field.name),
        )?;
        self.next_column += 1;
        Ok(())
    }

    fn append_null(&mut self) -> Result<(), String> {
        let index = self.next_field_index()?;
        let field = &self.fields[index];
        if !field.optional {
            return Err(format!(
                "Arrow batch row {} is missing required field '{}'",
                self.keep.len(),
                field.name
            ));
        }
        append_runtime_value_to_arrow(
            self.builders[index].as_mut(),
            &field.ty,
            None,
            &format!("Arrow batch row {} field '{}'", self.keep.len(), field.name),
        )?;
        self.next_column += 1;
        Ok(())
    }

    fn append_json_value(&mut self, value: &JsonValue) -> Result<(), String> {
        let index = self.next_field_index()?;
        let field = &self.fields[index];
        append_json_value_to_arrow(
            self.builders[index].as_mut(),
            &field.ty,
            value,
            &format!("field '{}'", field.name),
        )?;
        self.next_column += 1;
        Ok(())
    }

    fn append_avro_value(&mut self, value: &AvroValue) -> Result<(), String> {
        let index = self.next_field_index()?;
        let field = &self.fields[index];
        append_avro_value_to_arrow(
            self.builders[index].as_mut(),
            &field.ty,
            value,
            &format!("field '{}'", field.name),
        )?;
        self.next_column += 1;
        Ok(())
    }

    pub(crate) fn finish_row(&mut self) -> Result<(), String> {
        if self.next_column != self.fields.len() {
            return Err(format!(
                "Arrow batch row {} contains {} values for {} schema fields",
                self.keep.len(),
                self.next_column,
                self.fields.len()
            ));
        }
        self.keep.push(true);
        self.next_column = 0;
        Ok(())
    }

    /// Closes the row an append failed part-way through, so the batch keeps the rows around it.
    ///
    /// The columns the failed append had already filled keep the values it wrote, and the rest are
    /// filled with nulls, so every column still holds one whole value per row. `finish` then drops
    /// the row before the batch is checked against the schema, which is why the fill may write a
    /// null into a required column.
    pub(crate) fn abandon_row(&mut self) {
        for index in self.next_column..self.fields.len() {
            // A failed element append closes its own fixed-size list value, so a column that
            // already holds this row is left alone.
            if self.builders[index].len() > self.keep.len() {
                continue;
            }
            let field = &self.fields[index];
            append_runtime_value_to_arrow(
                self.builders[index].as_mut(),
                &field.ty,
                None,
                "abandoned Arrow batch row",
            )
            .assured("a null append cannot fail on a builder made from the field's own type");
        }
        self.keep.push(false);
        self.abandoned += 1;
        self.next_column = 0;
    }

    /// Drops every row after the first `kept`, for a caller that decoded rows it cannot use.
    pub(crate) fn abandon_rows_after(&mut self, kept: usize) {
        let mut keep_remaining = kept;
        for keep in &mut self.keep {
            if !*keep {
                continue;
            }
            match keep_remaining.checked_sub(1) {
                Some(remaining) => keep_remaining = remaining,
                None => {
                    *keep = false;
                    self.abandoned += 1;
                }
            }
        }
    }

    /// The rows the batch will contain: every row appended so far, less the abandoned ones.
    pub(crate) fn rows(&self) -> usize {
        self.keep
            .len()
            .checked_sub(self.abandoned)
            .assured("`abandoned` counts entries of `keep`, so it never exceeds their number")
    }

    pub(crate) fn finish(mut self) -> Result<RuntimeRecordBatch, String> {
        if self.next_column != 0 {
            return Err(format!(
                "Arrow batch row {} is incomplete with {} of {} schema fields",
                self.keep.len(),
                self.next_column,
                self.fields.len()
            ));
        }
        #[cfg(test)]
        RECORD_COLUMN_SETS_BUILT.with(|count| count.set(count.get() + 1));
        let rows = self.rows();
        let columns = self
            .builders
            .iter_mut()
            .map(|builder| builder.finish())
            .collect::<Vec<_>>();
        let columns = if self.abandoned == 0 {
            columns
        } else {
            // An abandoned row was closed with nulls so the columns stayed whole. Dropping it here
            // is what keeps a null out of a required column once the batch is checked.
            let keep = BooleanArray::from_iter(self.keep.iter().map(|keep| Some(*keep)));
            columns
                .iter()
                .map(|column| filter_arrow_array(column, &keep))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| error.to_string())?
        };
        let batch = if columns.is_empty() {
            RecordBatch::try_new_with_options(
                self.schema.clone(),
                columns,
                &RecordBatchOptions::new().with_row_count(Some(rows)),
            )
        } else {
            RecordBatch::try_new(self.schema.clone(), columns)
        }
        .map_err(|error| error.to_string())?;
        Ok(RuntimeRecordBatch { batch })
    }
}

/// An Arrow type that no Nervix type describes.
#[derive(Debug, Error)]
pub(crate) enum ArrowTypeError {
    #[error("runtime record materialization does not support Arrow type {data_type}")]
    Unsupported { data_type: ArrowDataType },
    #[error("fixed-size list length {len} is not a positive count")]
    EmptyFixedSizeList { len: i32 },
}

pub(crate) fn parse_as_type_from_arrow(
    data_type: &ArrowDataType,
) -> Result<ParseAsType, Report<ArrowTypeError>> {
    match data_type {
        ArrowDataType::UInt8 => Ok(ParseAsType::U8),
        ArrowDataType::Int8 => Ok(ParseAsType::I8),
        ArrowDataType::UInt16 => Ok(ParseAsType::U16),
        ArrowDataType::Int16 => Ok(ParseAsType::I16),
        ArrowDataType::UInt32 => Ok(ParseAsType::U32),
        ArrowDataType::Int32 => Ok(ParseAsType::I32),
        ArrowDataType::UInt64 => Ok(ParseAsType::U64),
        ArrowDataType::Int64 => Ok(ParseAsType::I64),
        ArrowDataType::Boolean => Ok(ParseAsType::Bool),
        ArrowDataType::Utf8 => Ok(ParseAsType::String),
        ArrowDataType::Timestamp(ArrowTimeUnit::Nanosecond, _) => Ok(ParseAsType::Datetime),
        ArrowDataType::Float32 => Ok(ParseAsType::F32),
        ArrowDataType::Float64 => Ok(ParseAsType::F64),
        ArrowDataType::List(element) => Ok(ParseAsType::Vec {
            element: Box::new(parse_as_type_from_arrow(element.data_type())?),
        }),
        ArrowDataType::FixedSizeList(element, len) => {
            let size = match u32::try_from(*len) {
                Ok(size) => NonZeroU32::new(size),
                Err(_) => None,
            };
            let Some(size) = size else {
                return Err(Report::new(ArrowTypeError::EmptyFixedSizeList {
                    len: *len,
                }));
            };
            Ok(ParseAsType::Array {
                element: Box::new(parse_as_type_from_arrow(element.data_type())?),
                len: size,
            })
        }
        other => Err(Report::new(ArrowTypeError::Unsupported {
            data_type: other.clone(),
        })),
    }
}

pub(crate) fn runtime_value_arrow_array(
    data_type: &ArrowDataType,
    value: Option<&RuntimeValue>,
    len: usize,
) -> Result<ArrayRef, String> {
    let ty = parse_as_type_from_arrow(data_type).map_err(|error| error.to_string())?;
    let mut builder = make_builder(data_type, len);
    for _ in 0..len {
        append_runtime_value_to_arrow(builder.as_mut(), &ty, value, "runtime scalar column")?;
    }
    Ok(builder.finish())
}

impl RuntimeRecordMetadata {
    pub fn from_ingested_at_watermarks(low: Timestamp, high: Timestamp) -> Self {
        Self {
            ingested_at_low_watermark: low,
            ingested_at_high_watermark: high,
        }
    }

    pub fn ingested_at_low_watermark(&self) -> Timestamp {
        self.ingested_at_low_watermark
    }

    pub fn ingested_at_high_watermark(&self) -> Timestamp {
        self.ingested_at_high_watermark
    }

    pub(crate) fn is_newer_than(&self, existing: &Self) -> bool {
        if self.ingested_at_high_watermark != existing.ingested_at_high_watermark {
            return self.ingested_at_high_watermark > existing.ingested_at_high_watermark;
        }
        self.ingested_at_low_watermark > existing.ingested_at_low_watermark
    }

    pub(crate) fn to_remote(&self) -> RemoteRuntimeRecordMetadata {
        RemoteRuntimeRecordMetadata {
            ingested_at_low_watermark: self.ingested_at_low_watermark,
            ingested_at_high_watermark: self.ingested_at_high_watermark,
        }
    }

    pub(crate) fn from_remote(metadata: RemoteRuntimeRecordMetadata) -> Self {
        Self {
            ingested_at_low_watermark: metadata.ingested_at_low_watermark,
            ingested_at_high_watermark: metadata.ingested_at_high_watermark,
        }
    }

    #[cfg(test)]
    pub(crate) fn test() -> Self {
        let watermark = Timestamp::from_unix_nanos(0);
        Self::from_ingested_at_watermarks(watermark, watermark)
    }
}

impl RuntimeValue {
    pub fn to_remote(&self) -> RemoteRuntimeValue {
        match self {
            Self::U8(v) => RemoteRuntimeValue::U8(*v),
            Self::I8(v) => RemoteRuntimeValue::I8(*v),
            Self::U16(v) => RemoteRuntimeValue::U16(*v),
            Self::I16(v) => RemoteRuntimeValue::I16(*v),
            Self::U32(v) => RemoteRuntimeValue::U32(*v),
            Self::I32(v) => RemoteRuntimeValue::I32(*v),
            Self::U64(v) => RemoteRuntimeValue::U64(*v),
            Self::I64(v) => RemoteRuntimeValue::I64(*v),
            Self::Bool(v) => RemoteRuntimeValue::Bool(*v),
            Self::String(v) => RemoteRuntimeValue::String(v.clone()),
            Self::Datetime(v) => RemoteRuntimeValue::Datetime(v.to_rfc3339()),
            Self::F32(v) => RemoteRuntimeValue::F32(v.into_inner()),
            Self::F64(v) => RemoteRuntimeValue::F64(v.into_inner()),
            Self::Array(v) => {
                RemoteRuntimeValue::Array(v.iter().map(RuntimeValue::to_remote_element).collect())
            }
            Self::Vec(v) => {
                RemoteRuntimeValue::Vec(v.iter().map(RuntimeValue::to_remote_element).collect())
            }
        }
    }

    pub fn from_remote(value: RemoteRuntimeValue) -> Self {
        match value {
            RemoteRuntimeValue::U8(v) => Self::U8(v),
            RemoteRuntimeValue::I8(v) => Self::I8(v),
            RemoteRuntimeValue::U16(v) => Self::U16(v),
            RemoteRuntimeValue::I16(v) => Self::I16(v),
            RemoteRuntimeValue::U32(v) => Self::U32(v),
            RemoteRuntimeValue::I32(v) => Self::I32(v),
            RemoteRuntimeValue::U64(v) => Self::U64(v),
            RemoteRuntimeValue::I64(v) => Self::I64(v),
            RemoteRuntimeValue::Bool(v) => Self::Bool(v),
            RemoteRuntimeValue::String(v) => Self::String(v),
            RemoteRuntimeValue::Datetime(v) => Self::Datetime(
                DateTime::parse_from_rfc3339(&v)
                    .assured("the peer renders these with to_rfc3339, which this parser accepts"),
            ),
            RemoteRuntimeValue::F32(v) => Self::F32(OrderedFloat(v)),
            RemoteRuntimeValue::F64(v) => Self::F64(OrderedFloat(v)),
            RemoteRuntimeValue::Array(v) => {
                Self::Array(v.into_iter().map(Self::from_remote_element).collect())
            }
            RemoteRuntimeValue::Vec(v) => {
                Self::Vec(v.into_iter().map(Self::from_remote_element).collect())
            }
        }
    }

    fn to_remote_element(&self) -> RemoteRuntimeElementValue {
        match self {
            Self::U8(v) => RemoteRuntimeElementValue::U8(*v),
            Self::I8(v) => RemoteRuntimeElementValue::I8(*v),
            Self::U16(v) => RemoteRuntimeElementValue::U16(*v),
            Self::I16(v) => RemoteRuntimeElementValue::I16(*v),
            Self::U32(v) => RemoteRuntimeElementValue::U32(*v),
            Self::I32(v) => RemoteRuntimeElementValue::I32(*v),
            Self::U64(v) => RemoteRuntimeElementValue::U64(*v),
            Self::I64(v) => RemoteRuntimeElementValue::I64(*v),
            Self::Bool(v) => RemoteRuntimeElementValue::Bool(*v),
            Self::String(v) => RemoteRuntimeElementValue::String(v.clone()),
            Self::Datetime(v) => RemoteRuntimeElementValue::Datetime(v.to_rfc3339()),
            Self::F32(v) => RemoteRuntimeElementValue::F32(v.into_inner()),
            Self::F64(v) => RemoteRuntimeElementValue::F64(v.into_inner()),
            Self::Array(values) => RemoteRuntimeElementValue::Array(
                values.iter().map(RuntimeValue::to_remote_element).collect(),
            ),
            Self::Vec(values) => RemoteRuntimeElementValue::Vec(
                values.iter().map(RuntimeValue::to_remote_element).collect(),
            ),
        }
    }

    fn from_remote_element(value: RemoteRuntimeElementValue) -> Self {
        match value {
            RemoteRuntimeElementValue::U8(v) => Self::U8(v),
            RemoteRuntimeElementValue::I8(v) => Self::I8(v),
            RemoteRuntimeElementValue::U16(v) => Self::U16(v),
            RemoteRuntimeElementValue::I16(v) => Self::I16(v),
            RemoteRuntimeElementValue::U32(v) => Self::U32(v),
            RemoteRuntimeElementValue::I32(v) => Self::I32(v),
            RemoteRuntimeElementValue::U64(v) => Self::U64(v),
            RemoteRuntimeElementValue::I64(v) => Self::I64(v),
            RemoteRuntimeElementValue::Bool(v) => Self::Bool(v),
            RemoteRuntimeElementValue::String(v) => Self::String(v),
            RemoteRuntimeElementValue::Datetime(v) => Self::Datetime(
                DateTime::parse_from_rfc3339(&v)
                    .assured("the peer renders these with to_rfc3339, which this parser accepts"),
            ),
            RemoteRuntimeElementValue::F32(v) => Self::F32(OrderedFloat(v)),
            RemoteRuntimeElementValue::F64(v) => Self::F64(OrderedFloat(v)),
            RemoteRuntimeElementValue::Array(values) => {
                Self::Array(values.into_iter().map(Self::from_remote_element).collect())
            }
            RemoteRuntimeElementValue::Vec(values) => {
                Self::Vec(values.into_iter().map(Self::from_remote_element).collect())
            }
        }
    }

    pub(crate) fn to_key_fragment(&self) -> String {
        match self {
            Self::String(v) => v.clone(),
            Self::Datetime(v) => v.to_rfc3339(),
            other => other.to_json_value().to_string(),
        }
    }

    pub(crate) fn to_json_value(&self) -> JsonValue {
        match self {
            Self::U8(v) => JsonValue::Number(JsonNumber::from(*v)),
            Self::I8(v) => JsonValue::Number(JsonNumber::from(*v)),
            Self::U16(v) => JsonValue::Number(JsonNumber::from(*v)),
            Self::I16(v) => JsonValue::Number(JsonNumber::from(*v)),
            Self::U32(v) => JsonValue::Number(JsonNumber::from(*v)),
            Self::I32(v) => JsonValue::Number(JsonNumber::from(*v)),
            Self::U64(v) => JsonValue::Number(JsonNumber::from(*v)),
            Self::I64(v) => JsonValue::Number(JsonNumber::from(*v)),
            Self::Bool(v) => JsonValue::Bool(*v),
            Self::String(v) => JsonValue::String(v.clone()),
            Self::Datetime(v) => JsonValue::String(v.to_rfc3339()),
            Self::F32(v) => {
                JsonValue::Number(JsonNumber::from_f64(f64::from(v.into_inner())).verified(
                    "the VM turns a non-finite float result into a row error, so a stored float \
                     is finite",
                ))
            }
            Self::F64(v) => JsonValue::Number(JsonNumber::from_f64(v.into_inner()).verified(
                "the VM turns a non-finite float result into a row error, so a stored float is \
                 finite",
            )),
            Self::Array(values) | Self::Vec(values) => {
                JsonValue::Array(values.iter().map(RuntimeValue::to_json_value).collect())
            }
        }
    }
}

impl Serialize for RuntimeValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        SerializableRuntimeValue::from(self).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for RuntimeValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = SerializableRuntimeValue::deserialize(deserializer)?;
        Self::try_from(value).map_err(serde::de::Error::custom)
    }
}

impl From<&RuntimeValue> for SerializableRuntimeValue {
    fn from(value: &RuntimeValue) -> Self {
        match value {
            RuntimeValue::U8(v) => Self::U8(*v),
            RuntimeValue::I8(v) => Self::I8(*v),
            RuntimeValue::U16(v) => Self::U16(*v),
            RuntimeValue::I16(v) => Self::I16(*v),
            RuntimeValue::U32(v) => Self::U32(*v),
            RuntimeValue::I32(v) => Self::I32(*v),
            RuntimeValue::U64(v) => Self::U64(*v),
            RuntimeValue::I64(v) => Self::I64(*v),
            RuntimeValue::Bool(v) => Self::Bool(*v),
            RuntimeValue::String(v) => Self::String(v.clone()),
            RuntimeValue::Datetime(v) => Self::Datetime(v.to_rfc3339()),
            RuntimeValue::F32(v) => Self::F32(v.into_inner()),
            RuntimeValue::F64(v) => Self::F64(v.into_inner()),
            RuntimeValue::Array(values) => Self::Array(values.iter().map(Self::from).collect()),
            RuntimeValue::Vec(values) => Self::Vec(values.iter().map(Self::from).collect()),
        }
    }
}

impl TryFrom<SerializableRuntimeValue> for RuntimeValue {
    type Error = String;

    fn try_from(value: SerializableRuntimeValue) -> Result<Self, Self::Error> {
        match value {
            SerializableRuntimeValue::U8(v) => Ok(Self::U8(v)),
            SerializableRuntimeValue::I8(v) => Ok(Self::I8(v)),
            SerializableRuntimeValue::U16(v) => Ok(Self::U16(v)),
            SerializableRuntimeValue::I16(v) => Ok(Self::I16(v)),
            SerializableRuntimeValue::U32(v) => Ok(Self::U32(v)),
            SerializableRuntimeValue::I32(v) => Ok(Self::I32(v)),
            SerializableRuntimeValue::U64(v) => Ok(Self::U64(v)),
            SerializableRuntimeValue::I64(v) => Ok(Self::I64(v)),
            SerializableRuntimeValue::Bool(v) => Ok(Self::Bool(v)),
            SerializableRuntimeValue::String(v) => Ok(Self::String(v)),
            SerializableRuntimeValue::Datetime(v) => DateTime::parse_from_rfc3339(&v)
                .map(Self::Datetime)
                .map_err(|error| error.to_string()),
            SerializableRuntimeValue::F32(v) => Ok(Self::F32(OrderedFloat(v))),
            SerializableRuntimeValue::F64(v) => Ok(Self::F64(OrderedFloat(v))),
            SerializableRuntimeValue::Array(values) => Ok(Self::Array(
                values
                    .into_iter()
                    .map(RuntimeValue::try_from)
                    .collect::<Result<Vec<_>, _>>()?,
            )),
            SerializableRuntimeValue::Vec(values) => Ok(Self::Vec(
                values
                    .into_iter()
                    .map(RuntimeValue::try_from)
                    .collect::<Result<Vec<_>, _>>()?,
            )),
        }
    }
}

pub fn compile_schema(schema: &CreateSchema) -> CompiledSchema {
    let fields = schema
        .fields
        .iter()
        .map(|field| CompiledSchemaField {
            name: field.name.as_str().to_string(),
            ty: field.ty.clone(),
            optional: field.optional,
            sensitive: field.sensitive,
        })
        .collect::<Vec<_>>();
    let arrow_fields = fields
        .iter()
        .map(|field| ArrowField::new(&field.name, arrow_data_type(&field.ty), field.optional))
        .collect::<Vec<_>>();
    CompiledSchema {
        fields,
        arrow_schema: StdArc::new(ArrowSchema::new(arrow_fields)),
    }
}

pub fn compile_codec(
    codec: &CreateCodec,
    schema: Arc<CompiledSchema>,
    wire_format: ResolvedCodecWireFormat<'_>,
) -> Result<Arc<CompiledCodec>, CodecError> {
    compile_codec_with_protobuf(codec, schema, wire_format, None)
}

pub fn compile_codec_with_protobuf(
    codec: &CreateCodec,
    schema: Arc<CompiledSchema>,
    wire_format: ResolvedCodecWireFormat<'_>,
    protobuf_descriptor: Option<MessageDescriptor>,
) -> Result<Arc<CompiledCodec>, CodecError> {
    let wire_schema = match wire_format {
        ResolvedCodecWireFormat::Json(schema_def) => {
            CompiledWireSchema::Json(compile_json_wire_schema(schema_def))
        }
        ResolvedCodecWireFormat::Cbor(schema_def) => {
            CompiledWireSchema::Cbor(compile_json_wire_schema(schema_def))
        }
        ResolvedCodecWireFormat::Avro(schema_def) => {
            let schema_json = avro_schema_json(schema_def, schema.fields());
            let parsed =
                AvroSchema::parse_str(&schema_json).map_err(|source| CodecError::InvalidCodec {
                    codec: codec.name.as_str().to_string(),
                    reason: source.to_string(),
                })?;
            let fields = schema_def
                .fields
                .iter()
                .enumerate()
                .map(|(position, field)| {
                    (
                        field.name.as_str().to_string(),
                        CompiledAvroWireField {
                            ty: field.ty,
                            optional: field.optional,
                            position,
                        },
                    )
                })
                .collect();
            CompiledWireSchema::Avro(CompiledAvroWireSchema {
                fields,
                schema: parsed,
            })
        }
        ResolvedCodecWireFormat::JaqNative {
            format,
            transformations,
        } => {
            if !transformations.has_any() {
                return Err(CodecError::InvalidCodec {
                    codec: codec.name.as_str().to_string(),
                    reason: "JAQ-native codec must declare a JAQ transformation".to_string(),
                });
            }
            CompiledWireSchema::JaqNative(CompiledJaqNativeCodec {
                format: JaqNativeFormat::from(format),
                transformations: CompiledJaqTransformations::compile(codec, transformations)?,
            })
        }
        ResolvedCodecWireFormat::Protobuf(config) => {
            if !config.transformations.has_any() {
                return Err(CodecError::InvalidCodec {
                    codec: codec.name.as_str().to_string(),
                    reason: "protobuf codec must declare a JAQ transformation".to_string(),
                });
            }
            let message = protobuf_descriptor.ok_or_else(|| CodecError::InvalidCodec {
                codec: codec.name.as_str().to_string(),
                reason: "protobuf codec is missing compiled descriptor".to_string(),
            })?;
            CompiledWireSchema::Protobuf(CompiledProtobufCodec {
                message,
                transformations: CompiledJaqTransformations::compile(
                    codec,
                    &config.transformations,
                )?,
            })
        }
        ResolvedCodecWireFormat::Syslog => {
            syslog::validate_compiled_schema(codec, &schema)?;
            CompiledWireSchema::Syslog
        }
    };

    Ok(Arc::new(CompiledCodec {
        name: ModelName::from(&codec.name),
        schema,
        wire_schema,
    }))
}

fn compile_json_wire_schema(schema_def: &CreateWireSchema<JsonType>) -> CompiledJsonWireSchema {
    let fields = schema_def
        .fields
        .iter()
        .map(|field| {
            (
                field.name.as_str().to_string(),
                CompiledJsonWireField {
                    ty: field.ty,
                    optional: field.optional,
                },
            )
        })
        .collect();
    CompiledJsonWireSchema {
        strictness: schema_def.strictness,
        fields,
    }
}

/// Appends one payload as one row of `builder`.
///
/// The builder belongs to the batch the row joins, so a batch of `n` payloads is decoded into one
/// set of Arrow columns. A payload that fails to decode leaves the batch exactly as it was: the row
/// it had started is closed and dropped, and the error names the payload that produced it.
///
/// A caller that already owns its payload hands it over, which is what lets JSON parse in place.
pub(crate) fn decode_with_codec(
    codec: &CompiledCodec,
    mut payload: Cow<'_, [u8]>,
    builder: &mut RuntimeRecordBatchBuilder,
) -> Result<(), CodecError> {
    let appended = match &codec.wire_schema {
        CompiledWireSchema::Json(wire_schema) => {
            decode_json(codec, wire_schema, &mut payload, builder)
        }
        CompiledWireSchema::Cbor(wire_schema) => decode_cbor(codec, wire_schema, &payload, builder),
        CompiledWireSchema::Avro(wire_schema) => decode_avro(codec, wire_schema, &payload, builder),
        CompiledWireSchema::JaqNative(native) => transform_jaq_native(codec, native, &payload)
            .and_then(|value| decode_json_value(codec, &value, None, builder)),
        CompiledWireSchema::Protobuf(protobuf) => transform_protobuf(codec, protobuf, &payload)
            .and_then(|value| decode_json_value(codec, &value, None, builder)),
        CompiledWireSchema::Syslog => syslog::decode(codec, &payload, builder),
    };
    finish_decoded_row(codec, builder, appended)
}

/// Commits the row a codec appended, or closes and drops the row a failed decode left behind.
fn finish_decoded_row(
    codec: &CompiledCodec,
    builder: &mut RuntimeRecordBatchBuilder,
    appended: Result<(), CodecError>,
) -> Result<(), CodecError> {
    match appended {
        Ok(()) => builder
            .finish_row()
            .map_err(|reason| CodecError::InvalidCodec {
                codec: codec.name.as_str().to_string(),
                reason,
            }),
        Err(error) => {
            builder.abandon_row();
            Err(error)
        }
    }
}

struct ArrowCodecRow<'a> {
    codec: &'a CompiledCodec,
    batch: &'a RuntimeRecordBatch,
    row_index: usize,
}

impl<'a> ArrowCodecRow<'a> {
    fn new(codec: &'a CompiledCodec, batch: &'a RuntimeRecordBatch, row_index: usize) -> Self {
        Self {
            codec,
            batch,
            row_index,
        }
    }

    fn value(&self, field_index: usize) -> ArrowCodecValue<'a> {
        let field = &self.codec.schema.fields[field_index];
        ArrowCodecValue {
            codec: self.codec,
            array: self.batch.batch.column(field_index).as_ref(),
            ty: &field.ty,
            field: &field.name,
            row_index: self.row_index,
        }
    }

    fn to_json_value(&self) -> Result<JsonValue, CodecError> {
        serde_json::to_value(self).map_err(|source| CodecError::JsonEncode {
            codec: self.codec.name.as_str().to_string(),
            source,
        })
    }

    fn to_avro_record(
        &self,
        wire_schema: &CompiledAvroWireSchema,
    ) -> Result<AvroValue, CodecError> {
        let mut fields = Vec::with_capacity(self.codec.schema.fields.len());
        for (field_index, field) in self.codec.schema.fields.iter().enumerate() {
            let wire_field =
                wire_schema
                    .fields
                    .get(&field.name)
                    .ok_or_else(|| CodecError::InvalidCodec {
                        codec: self.codec.name.as_str().to_string(),
                        reason: format!("missing wire field '{}'", field.name),
                    })?;
            fields.push((
                field.name.clone(),
                self.value(field_index)
                    .to_avro_wire_value(wire_field.ty, wire_field.optional)?,
            ));
        }
        Ok(AvroValue::Record(fields))
    }
}

impl Serialize for ArrowCodecRow<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(None)?;
        for (field_index, field) in self.codec.schema.fields.iter().enumerate() {
            let value = self.value(field_index);
            if value.is_null() && field.optional {
                continue;
            }
            map.serialize_entry(&field.name, &value)?;
        }
        map.end()
    }
}

struct ArrowCodecValue<'a> {
    codec: &'a CompiledCodec,
    array: &'a dyn Array,
    ty: &'a ParseAsType,
    field: &'a str,
    row_index: usize,
}

impl<'a> ArrowCodecValue<'a> {
    fn is_null(&self) -> bool {
        self.array.is_null(self.row_index)
    }

    fn typed<T: 'static>(&self, arrow_type: &str) -> Result<&'a T, String> {
        self.array.as_any().downcast_ref::<T>().ok_or_else(|| {
            format!(
                "field '{}' is not a {arrow_type} at row {}",
                self.field, self.row_index
            )
        })
    }

    fn sequence(&self) -> Result<ArrowCodecSequence<'a>, String> {
        match self.ty {
            ParseAsType::Vec { element } => {
                let array = self.typed::<ListArray>("ListArray")?;
                let offsets = array.value_offsets();
                let start = usize::try_from(offsets[self.row_index])
                    .map_err(|_| format!("field '{}' has a negative list offset", self.field))?;
                let end = usize::try_from(offsets[self.row_index + 1])
                    .map_err(|_| format!("field '{}' has a negative list offset", self.field))?;
                Ok(ArrowCodecSequence {
                    codec: self.codec,
                    array: array.values().as_ref(),
                    element,
                    field: self.field,
                    rows: start..end,
                })
            }
            ParseAsType::Array { element, len } => {
                let array = self.typed::<FixedSizeListArray>("FixedSizeListArray")?;
                if array.value_length() != i32::try_from(len.get()).unwrap_or(i32::MAX) {
                    return Err(format!(
                        "field '{}' fixed-size list length {} does not match schema length {}",
                        self.field,
                        array.value_length(),
                        len
                    ));
                }
                let start = usize::try_from(array.value_offset(self.row_index)).map_err(|_| {
                    format!(
                        "field '{}' has a negative fixed-size list offset",
                        self.field
                    )
                })?;
                let end = start
                    .checked_add(len.get().arch_into())
                    .verified("the offset and length address one fixed-size list already decoded");
                Ok(ArrowCodecSequence {
                    codec: self.codec,
                    array: array.values().as_ref(),
                    element,
                    field: self.field,
                    rows: start..end,
                })
            }
            _ => Err(format!("field '{}' is not an array or vector", self.field)),
        }
    }

    fn encode_field_error(&self, reason: impl Into<String>) -> CodecError {
        CodecError::EncodeField {
            codec: self.codec.name.as_str().to_string(),
            field: self.field.to_string(),
            reason: reason.into(),
        }
    }

    fn to_avro_wire_value(
        &self,
        wire_ty: AvroType,
        optional: bool,
    ) -> Result<AvroValue, CodecError> {
        if self.is_null() {
            if optional {
                return Ok(AvroValue::Union(0, Box::new(AvroValue::Null)));
            }
            return Err(self
                .encode_field_error(format!("required field is null at row {}", self.row_index)));
        }

        let value = match wire_ty {
            AvroType::Boolean => self.to_avro_boolean(),
            AvroType::Int => self.to_avro_int(),
            AvroType::Long => self.to_avro_long(),
            AvroType::Float => self.to_avro_float(),
            AvroType::Double => self.to_avro_double(),
            AvroType::String => self.to_avro_string(),
            AvroType::Array => self.to_avro_array(),
            unsupported => {
                Err(self.encode_field_error(format!("unsupported avro type {unsupported:?}")))
            }
        }?;
        if optional {
            Ok(AvroValue::Union(1, Box::new(value)))
        } else {
            Ok(value)
        }
    }

    fn to_avro_boolean(&self) -> Result<AvroValue, CodecError> {
        if let ParseAsType::Bool = self.ty {
            return self
                .typed::<BooleanArray>("BooleanArray")
                .map(|array| AvroValue::Boolean(array.value(self.row_index)))
                .map_err(|reason| self.encode_field_error(reason));
        }
        Err(self.encode_field_error("expected bool"))
    }

    fn to_avro_int(&self) -> Result<AvroValue, CodecError> {
        let value = match self.ty {
            ParseAsType::I8 => i32::from(
                self.typed::<Int8Array>("Int8Array")
                    .map_err(|reason| self.encode_field_error(reason))?
                    .value(self.row_index),
            ),
            ParseAsType::I16 => i32::from(
                self.typed::<Int16Array>("Int16Array")
                    .map_err(|reason| self.encode_field_error(reason))?
                    .value(self.row_index),
            ),
            ParseAsType::I32 => self
                .typed::<Int32Array>("Int32Array")
                .map_err(|reason| self.encode_field_error(reason))?
                .value(self.row_index),
            ParseAsType::U8 => i32::from(
                self.typed::<UInt8Array>("UInt8Array")
                    .map_err(|reason| self.encode_field_error(reason))?
                    .value(self.row_index),
            ),
            ParseAsType::U16 => i32::from(
                self.typed::<UInt16Array>("UInt16Array")
                    .map_err(|reason| self.encode_field_error(reason))?
                    .value(self.row_index),
            ),
            ParseAsType::U32 => i32::try_from(
                self.typed::<UInt32Array>("UInt32Array")
                    .map_err(|reason| self.encode_field_error(reason))?
                    .value(self.row_index),
            )
            .map_err(|_| self.encode_field_error("U32 value does not fit Avro INT"))?,
            _ => return Err(self.encode_field_error("expected int-compatible value")),
        };
        Ok(AvroValue::Int(value))
    }

    fn to_avro_long(&self) -> Result<AvroValue, CodecError> {
        let value = match self.ty {
            ParseAsType::I8 => i64::from(
                self.typed::<Int8Array>("Int8Array")
                    .map_err(|reason| self.encode_field_error(reason))?
                    .value(self.row_index),
            ),
            ParseAsType::I16 => i64::from(
                self.typed::<Int16Array>("Int16Array")
                    .map_err(|reason| self.encode_field_error(reason))?
                    .value(self.row_index),
            ),
            ParseAsType::I32 => i64::from(
                self.typed::<Int32Array>("Int32Array")
                    .map_err(|reason| self.encode_field_error(reason))?
                    .value(self.row_index),
            ),
            ParseAsType::I64 => self
                .typed::<Int64Array>("Int64Array")
                .map_err(|reason| self.encode_field_error(reason))?
                .value(self.row_index),
            ParseAsType::U8 => i64::from(
                self.typed::<UInt8Array>("UInt8Array")
                    .map_err(|reason| self.encode_field_error(reason))?
                    .value(self.row_index),
            ),
            ParseAsType::U16 => i64::from(
                self.typed::<UInt16Array>("UInt16Array")
                    .map_err(|reason| self.encode_field_error(reason))?
                    .value(self.row_index),
            ),
            ParseAsType::U32 => i64::from(
                self.typed::<UInt32Array>("UInt32Array")
                    .map_err(|reason| self.encode_field_error(reason))?
                    .value(self.row_index),
            ),
            ParseAsType::U64 => i64::try_from(
                self.typed::<UInt64Array>("UInt64Array")
                    .map_err(|reason| self.encode_field_error(reason))?
                    .value(self.row_index),
            )
            .map_err(|_| self.encode_field_error("U64 value does not fit Avro LONG"))?,
            _ => return Err(self.encode_field_error("expected long-compatible value")),
        };
        Ok(AvroValue::Long(value))
    }

    fn to_avro_float(&self) -> Result<AvroValue, CodecError> {
        if let ParseAsType::F32 = self.ty {
            return self
                .typed::<Float32Array>("Float32Array")
                .map(|array| AvroValue::Float(array.value(self.row_index)))
                .map_err(|reason| self.encode_field_error(reason));
        }
        Err(self.encode_field_error("expected f32"))
    }

    fn to_avro_double(&self) -> Result<AvroValue, CodecError> {
        match self.ty {
            ParseAsType::F32 => self
                .typed::<Float32Array>("Float32Array")
                .map(|array| AvroValue::Double(f64::from(array.value(self.row_index))))
                .map_err(|reason| self.encode_field_error(reason)),
            ParseAsType::F64 => self
                .typed::<Float64Array>("Float64Array")
                .map(|array| AvroValue::Double(array.value(self.row_index)))
                .map_err(|reason| self.encode_field_error(reason)),
            _ => Err(self.encode_field_error("expected float-compatible value")),
        }
    }

    fn to_avro_string(&self) -> Result<AvroValue, CodecError> {
        match self.ty {
            ParseAsType::String => self
                .typed::<StringArray>("StringArray")
                .map(|array| AvroValue::String(array.value(self.row_index).to_string()))
                .map_err(|reason| self.encode_field_error(reason)),
            ParseAsType::Datetime => self
                .typed::<TimestampNanosecondArray>("TimestampNanosecondArray")
                .map(|array| {
                    AvroValue::String(
                        DateTime::from_timestamp_nanos(array.value(self.row_index))
                            .fixed_offset()
                            .to_rfc3339(),
                    )
                })
                .map_err(|reason| self.encode_field_error(reason)),
            _ => Err(self.encode_field_error("expected string-compatible value")),
        }
    }

    fn to_avro_array(&self) -> Result<AvroValue, CodecError> {
        self.sequence()
            .map_err(|reason| self.encode_field_error(reason))?
            .to_avro_values()
            .map(AvroValue::Array)
    }

    fn to_avro_array_item(&self) -> Result<AvroValue, CodecError> {
        if self.is_null() {
            return Err(
                self.encode_field_error(format!("list contains null at index {}", self.row_index))
            );
        }
        match self.ty {
            ParseAsType::Bool => self.to_avro_boolean(),
            ParseAsType::U8
            | ParseAsType::I8
            | ParseAsType::U16
            | ParseAsType::I16
            | ParseAsType::U32
            | ParseAsType::I32
            | ParseAsType::U64
            | ParseAsType::I64 => self.to_avro_long(),
            ParseAsType::F32 => self.to_avro_float(),
            ParseAsType::F64 => self.to_avro_double(),
            ParseAsType::String | ParseAsType::Datetime => self.to_avro_string(),
            ParseAsType::Array { .. } | ParseAsType::Vec { .. } => self.to_avro_array(),
        }
    }
}

impl Serialize for ArrowCodecValue<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if self.is_null() {
            return Err(serde::ser::Error::custom(format!(
                "field '{}' contains null at row {}",
                self.field, self.row_index
            )));
        }

        macro_rules! serialize_primitive {
            ($array:ty, $arrow_type:literal, $method:ident) => {{
                let array = self
                    .typed::<$array>($arrow_type)
                    .map_err(serde::ser::Error::custom)?;
                serializer.$method(array.value(self.row_index))
            }};
        }

        match self.ty {
            ParseAsType::U8 => serialize_primitive!(UInt8Array, "UInt8Array", serialize_u8),
            ParseAsType::I8 => serialize_primitive!(Int8Array, "Int8Array", serialize_i8),
            ParseAsType::U16 => serialize_primitive!(UInt16Array, "UInt16Array", serialize_u16),
            ParseAsType::I16 => serialize_primitive!(Int16Array, "Int16Array", serialize_i16),
            ParseAsType::U32 => serialize_primitive!(UInt32Array, "UInt32Array", serialize_u32),
            ParseAsType::I32 => serialize_primitive!(Int32Array, "Int32Array", serialize_i32),
            ParseAsType::U64 => serialize_primitive!(UInt64Array, "UInt64Array", serialize_u64),
            ParseAsType::I64 => serialize_primitive!(Int64Array, "Int64Array", serialize_i64),
            ParseAsType::Bool => {
                serialize_primitive!(BooleanArray, "BooleanArray", serialize_bool)
            }
            ParseAsType::String => {
                let array = self
                    .typed::<StringArray>("StringArray")
                    .map_err(serde::ser::Error::custom)?;
                serializer.serialize_str(array.value(self.row_index))
            }
            ParseAsType::Datetime => {
                let array = self
                    .typed::<TimestampNanosecondArray>("TimestampNanosecondArray")
                    .map_err(serde::ser::Error::custom)?;
                serializer.serialize_str(
                    &DateTime::from_timestamp_nanos(array.value(self.row_index))
                        .fixed_offset()
                        .to_rfc3339(),
                )
            }
            ParseAsType::F32 => {
                serialize_primitive!(Float32Array, "Float32Array", serialize_f32)
            }
            ParseAsType::F64 => {
                serialize_primitive!(Float64Array, "Float64Array", serialize_f64)
            }
            ParseAsType::Array { .. } | ParseAsType::Vec { .. } => self
                .sequence()
                .map_err(serde::ser::Error::custom)?
                .serialize(serializer),
        }
    }
}

struct ArrowCodecSequence<'a> {
    codec: &'a CompiledCodec,
    array: &'a dyn Array,
    element: &'a ParseAsType,
    field: &'a str,
    rows: std::ops::Range<usize>,
}

impl ArrowCodecSequence<'_> {
    fn to_avro_values(&self) -> Result<Vec<AvroValue>, CodecError> {
        self.rows
            .clone()
            .map(|row_index| {
                ArrowCodecValue {
                    codec: self.codec,
                    array: self.array,
                    ty: self.element,
                    field: self.field,
                    row_index,
                }
                .to_avro_array_item()
            })
            .collect()
    }
}

impl Serialize for ArrowCodecSequence<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut sequence = serializer.serialize_seq(Some(self.rows.len()))?;
        for row_index in self.rows.clone() {
            sequence.serialize_element(&ArrowCodecValue {
                codec: self.codec,
                array: self.array,
                ty: self.element,
                field: self.field,
                row_index,
            })?;
        }
        sequence.end()
    }
}

fn decode_json(
    codec: &CompiledCodec,
    wire_schema: &CompiledJsonWireSchema,
    payload: &mut Cow<'_, [u8]>,
    builder: &mut RuntimeRecordBatchBuilder,
) -> Result<(), CodecError> {
    // An owned payload is scratch space the parser may overwrite, which is what simd-json needs.
    let value = match payload {
        Cow::Owned(payload) => simd_json::from_slice::<JsonValue>(payload).map_err(|source| {
            CodecError::SimdJsonDecode {
                codec: codec.name.as_str().to_string(),
                source,
            }
        })?,
        Cow::Borrowed(payload) => {
            serde_json::from_slice::<JsonValue>(payload).map_err(|source| {
                CodecError::JsonDecode {
                    codec: codec.name.as_str().to_string(),
                    source,
                }
            })?
        }
    };
    decode_json_value(codec, &value, Some(wire_schema), builder)
}

fn decode_cbor(
    codec: &CompiledCodec,
    wire_schema: &CompiledJsonWireSchema,
    payload: &[u8],
    builder: &mut RuntimeRecordBatchBuilder,
) -> Result<(), CodecError> {
    let value = ciborium::from_reader::<JsonValue, _>(Cursor::new(payload)).map_err(|source| {
        CodecError::CborDecode {
            codec: codec.name.as_str().to_string(),
            reason: source.to_string(),
        }
    })?;
    decode_json_value(codec, &value, Some(wire_schema), builder)
}

fn transform_jaq_native(
    codec: &CompiledCodec,
    native: &CompiledJaqNativeCodec,
    payload: &[u8],
) -> Result<JsonValue, CodecError> {
    let Some(program) = native.transformations.on_ingestion.as_deref() else {
        return Err(CodecError::InvalidCodec {
            codec: codec.name.as_str().to_string(),
            reason: "JAQ-native codec used for decoding must declare ON INGESTION transformation"
                .to_string(),
        });
    };
    let value =
        native
            .format
            .read_single_value(payload)
            .map_err(|error| CodecError::JaqNativeDecode {
                codec: codec.name.as_str().to_string(),
                format: native.format.name(),
                reason: error.to_string(),
            })?;
    run_jaq_transformation(codec, program, value)
}

fn transform_protobuf(
    codec: &CompiledCodec,
    protobuf: &CompiledProtobufCodec,
    payload: &[u8],
) -> Result<JsonValue, CodecError> {
    let Some(program) = protobuf.transformations.on_ingestion.as_deref() else {
        return Err(CodecError::InvalidCodec {
            codec: codec.name.as_str().to_string(),
            reason: "protobuf codec used for decoding must declare ON INGESTION transformation"
                .to_string(),
        });
    };
    let value = decode_protobuf_payload(&protobuf.message, payload).map_err(|reason| {
        CodecError::ProtobufDecode {
            codec: codec.name.as_str().to_string(),
            reason,
        }
    })?;
    run_jaq_transformation(codec, program, value)
}

/// Decode protobuf bytes as `message` into the JSON value jaq programs operate on.
pub(crate) fn decode_protobuf_payload(
    message: &MessageDescriptor,
    payload: &[u8],
) -> Result<JsonValue, String> {
    let message =
        DynamicMessage::decode(message.clone(), payload).map_err(|source| source.to_string())?;
    protobuf_message_to_json(&message)
}

/// Encode a JSON value as protobuf bytes for `message`.
pub(crate) fn encode_protobuf_payload(
    message: &MessageDescriptor,
    value: &JsonValue,
) -> Result<Vec<u8>, String> {
    let encoded_json = serde_json::to_vec(value).map_err(|source| source.to_string())?;
    let mut deserializer = serde_json::Deserializer::from_slice(&encoded_json);
    let options = ProtobufDeserializeOptions::new().deny_unknown_fields(true);
    let message =
        DynamicMessage::deserialize_with_options(message.clone(), &mut deserializer, &options)
            .map_err(|source| source.to_string())?;
    deserializer.end().map_err(|source| source.to_string())?;
    let mut encoded = Vec::new();
    message
        .encode(&mut encoded)
        .map_err(|source| source.to_string())?;
    Ok(encoded)
}

fn protobuf_message_to_json(message: &DynamicMessage) -> Result<JsonValue, String> {
    let mut encoded = Vec::new();
    let mut serializer = serde_json::Serializer::new(&mut encoded);
    let options = ProtobufSerializeOptions::new()
        .use_proto_field_name(true)
        .stringify_64_bit_integers(false);
    message
        .serialize_with_options(&mut serializer, &options)
        .map_err(|source| source.to_string())?;
    serde_json::from_slice(&encoded).map_err(|source| source.to_string())
}

fn decode_json_value(
    codec: &CompiledCodec,
    value: &JsonValue,
    wire_schema: Option<&CompiledJsonWireSchema>,
    builder: &mut RuntimeRecordBatchBuilder,
) -> Result<(), CodecError> {
    let JsonValue::Object(object) = value else {
        return Err(CodecError::ExpectedObject {
            codec: codec.name.as_str().to_string(),
        });
    };

    if let Some(wire_schema) = wire_schema
        && !wire_schema.strictness.allows_unknown_fields()
    {
        for field in object.keys() {
            if !wire_schema.fields.contains_key(field) {
                return Err(CodecError::UnexpectedField {
                    codec: codec.name.as_str().to_string(),
                    field: field.clone(),
                });
            }
        }
    }

    for field in codec.schema.fields() {
        let wire_field =
            wire_schema.and_then(|wire_schema| wire_schema.fields.get(&field.name).copied());
        if wire_schema.is_some() && wire_field.is_none() {
            return Err(CodecError::InvalidCodec {
                codec: codec.name.as_str().to_string(),
                reason: format!("missing wire field '{}'", field.name),
            });
        }
        let Some(value) = object.get(&field.name) else {
            if field.optional && wire_field.is_none_or(|wire_field| wire_field.optional) {
                builder
                    .append_null()
                    .map_err(|reason| CodecError::InvalidCodec {
                        codec: codec.name.as_str().to_string(),
                        reason,
                    })?;
                continue;
            }
            return Err(CodecError::MissingField {
                codec: codec.name.as_str().to_string(),
                field: field.name.clone(),
            });
        };
        if value.is_null() {
            if field.optional && wire_field.is_none_or(|wire_field| wire_field.optional) {
                builder
                    .append_null()
                    .map_err(|reason| CodecError::InvalidCodec {
                        codec: codec.name.as_str().to_string(),
                        reason,
                    })?;
                continue;
            }
            return Err(CodecError::ParseField {
                codec: codec.name.as_str().to_string(),
                field: field.name.clone(),
                reason: "null is incompatible with required field".to_string(),
            });
        }
        if let Some(wire_field) = wire_field
            && !json_value_matches_wire_type(value, wire_field.ty)
        {
            return Err(CodecError::ParseField {
                codec: codec.name.as_str().to_string(),
                field: field.name.clone(),
                reason: format!("expected {:?}, found {}", wire_field.ty, value),
            });
        }
        builder
            .append_json_value(value)
            .map_err(|reason| CodecError::ParseField {
                codec: codec.name.as_str().to_string(),
                field: field.name.clone(),
                reason,
            })?;
    }
    Ok(())
}

fn run_jaq_transformation(
    codec: &CompiledCodec,
    program: &CompiledJaqProgram,
    input: JsonValue,
) -> Result<JsonValue, CodecError> {
    program
        .run_single(input)
        .map_err(|error| CodecError::JaqTransform {
            codec: codec.name.as_str().to_string(),
            reason: error.to_string(),
        })
}

fn decode_avro(
    codec: &CompiledCodec,
    wire_schema: &CompiledAvroWireSchema,
    payload: &[u8],
    builder: &mut RuntimeRecordBatchBuilder,
) -> Result<(), CodecError> {
    let mut cursor = Cursor::new(payload);
    let value = from_avro_datum(&wire_schema.schema, &mut cursor, None).map_err(|source| {
        CodecError::AvroDecode {
            codec: codec.name.as_str().to_string(),
            source,
        }
    })?;
    let AvroValue::Record(values) = value else {
        return Err(CodecError::ExpectedObject {
            codec: codec.name.as_str().to_string(),
        });
    };

    for field in codec.schema.fields() {
        let wire_field =
            wire_schema
                .fields
                .get(&field.name)
                .ok_or_else(|| CodecError::InvalidCodec {
                    codec: codec.name.as_str().to_string(),
                    reason: format!("missing wire field '{}'", field.name),
                })?;
        let Some((name, value)) = values.get(wire_field.position) else {
            if field.optional && wire_field.optional {
                builder
                    .append_null()
                    .map_err(|reason| CodecError::InvalidCodec {
                        codec: codec.name.as_str().to_string(),
                        reason,
                    })?;
                continue;
            }
            return Err(CodecError::MissingField {
                codec: codec.name.as_str().to_string(),
                field: field.name.clone(),
            });
        };
        if name != &field.name {
            return Err(CodecError::InvalidCodec {
                codec: codec.name.as_str().to_string(),
                reason: format!(
                    "wire field position {} contains '{}' instead of '{}'",
                    wire_field.position, name, field.name
                ),
            });
        }
        if avro_value_is_null(value) {
            if field.optional && wire_field.optional {
                builder
                    .append_null()
                    .map_err(|reason| CodecError::InvalidCodec {
                        codec: codec.name.as_str().to_string(),
                        reason,
                    })?;
                continue;
            }
            return Err(CodecError::ParseField {
                codec: codec.name.as_str().to_string(),
                field: field.name.clone(),
                reason: "null is incompatible with required field".to_string(),
            });
        }
        builder
            .append_avro_value(value)
            .map_err(|reason| CodecError::ParseField {
                codec: codec.name.as_str().to_string(),
                field: field.name.clone(),
                reason,
            })?;
    }
    Ok(())
}

fn append_json_value_to_arrow(
    builder: &mut dyn ArrayBuilder,
    ty: &ParseAsType,
    value: &JsonValue,
    context: &str,
) -> Result<(), String> {
    let incompatible = || format!("{context} value {value} is incompatible with {ty:?}");

    macro_rules! append_primitive {
        ($builder:ty, $parsed:expr) => {{
            let parsed = ($parsed).ok_or_else(&incompatible)?;
            typed_arrow_builder::<$builder>(builder, context)?.append_value(parsed);
            Ok(())
        }};
    }

    match ty {
        ParseAsType::U8 => append_primitive!(
            UInt8Builder,
            value.as_u64().and_then(|value| u8::try_from(value).ok())
        ),
        ParseAsType::I8 => append_primitive!(
            Int8Builder,
            value.as_i64().and_then(|value| i8::try_from(value).ok())
        ),
        ParseAsType::U16 => append_primitive!(
            UInt16Builder,
            value.as_u64().and_then(|value| u16::try_from(value).ok())
        ),
        ParseAsType::I16 => append_primitive!(
            Int16Builder,
            value.as_i64().and_then(|value| i16::try_from(value).ok())
        ),
        ParseAsType::U32 => append_primitive!(
            UInt32Builder,
            value.as_u64().and_then(|value| u32::try_from(value).ok())
        ),
        ParseAsType::I32 => append_primitive!(
            Int32Builder,
            value.as_i64().and_then(|value| i32::try_from(value).ok())
        ),
        ParseAsType::U64 => append_primitive!(UInt64Builder, value.as_u64()),
        ParseAsType::I64 => append_primitive!(Int64Builder, value.as_i64()),
        ParseAsType::Bool => append_primitive!(BooleanBuilder, value.as_bool()),
        ParseAsType::String => append_primitive!(StringBuilder, value.as_str()),
        ParseAsType::Datetime => append_primitive!(
            TimestampNanosecondBuilder,
            if let Some(value) = value.as_str()
                && let Ok(value) = DateTime::parse_from_rfc3339(value)
            {
                value.timestamp_nanos_opt()
            } else {
                None
            }
        ),
        ParseAsType::F32 => {
            append_primitive!(Float32Builder, value.as_f64().map(ApproxInto::approx_into))
        }
        ParseAsType::F64 => append_primitive!(Float64Builder, value.as_f64()),
        ParseAsType::Array { element, len } => {
            let values = value.as_array().ok_or_else(&incompatible)?;
            if values.len() != len.get().arch_into() {
                return Err(incompatible());
            }
            let builder = typed_arrow_builder::<FixedSizeListBuilder<Box<dyn ArrayBuilder>>>(
                builder, context,
            )?;
            for (index, value) in values.iter().enumerate() {
                if let Err(error) = append_json_value_to_arrow(
                    builder.values().as_mut(),
                    element,
                    value,
                    &format!("{context}[{index}]"),
                ) {
                    close_partial_fixed_size_list(builder, element, len.get().arch_into());
                    return Err(error);
                }
            }
            builder.append(true);
            Ok(())
        }
        ParseAsType::Vec { element } => {
            let values = value.as_array().ok_or_else(&incompatible)?;
            let builder =
                typed_arrow_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(builder, context)?;
            for (index, value) in values.iter().enumerate() {
                append_json_value_to_arrow(
                    builder.values().as_mut(),
                    element,
                    value,
                    &format!("{context}[{index}]"),
                )?;
            }
            builder.append(true);
            Ok(())
        }
    }
}

fn append_avro_value_to_arrow(
    builder: &mut dyn ArrayBuilder,
    ty: &ParseAsType,
    value: &AvroValue,
    context: &str,
) -> Result<(), String> {
    let value = avro_value_payload(value);
    let incompatible = || format!("{context} {value:?} is incompatible with {ty:?}");

    macro_rules! append_primitive {
        ($builder:ty, $parsed:expr) => {{
            let parsed = ($parsed).ok_or_else(&incompatible)?;
            typed_arrow_builder::<$builder>(builder, context)?.append_value(parsed);
            Ok(())
        }};
    }

    match ty {
        ParseAsType::U8 => append_primitive!(
            UInt8Builder,
            avro_to_u64(value).and_then(|value| u8::try_from(value).ok())
        ),
        ParseAsType::I8 => append_primitive!(
            Int8Builder,
            avro_to_i64(value).and_then(|value| i8::try_from(value).ok())
        ),
        ParseAsType::U16 => append_primitive!(
            UInt16Builder,
            avro_to_u64(value).and_then(|value| u16::try_from(value).ok())
        ),
        ParseAsType::I16 => append_primitive!(
            Int16Builder,
            avro_to_i64(value).and_then(|value| i16::try_from(value).ok())
        ),
        ParseAsType::U32 => append_primitive!(
            UInt32Builder,
            avro_to_u64(value).and_then(|value| u32::try_from(value).ok())
        ),
        ParseAsType::I32 => append_primitive!(
            Int32Builder,
            avro_to_i64(value).and_then(|value| i32::try_from(value).ok())
        ),
        ParseAsType::U64 => append_primitive!(UInt64Builder, avro_to_u64(value)),
        ParseAsType::I64 => append_primitive!(Int64Builder, avro_to_i64(value)),
        ParseAsType::Bool => append_primitive!(
            BooleanBuilder,
            if let AvroValue::Boolean(value) = value {
                Some(*value)
            } else {
                None
            }
        ),
        ParseAsType::String => append_primitive!(
            StringBuilder,
            if let AvroValue::String(value) = value {
                Some(value.as_str())
            } else {
                None
            }
        ),
        ParseAsType::Datetime => append_primitive!(
            TimestampNanosecondBuilder,
            if let AvroValue::String(value) = value {
                if let Ok(value) = DateTime::parse_from_rfc3339(value) {
                    value.timestamp_nanos_opt()
                } else {
                    None
                }
            } else {
                None
            }
        ),
        ParseAsType::F32 => append_primitive!(
            Float32Builder,
            if let AvroValue::Float(value) = value {
                Some(*value)
            } else {
                None
            }
        ),
        ParseAsType::F64 => append_primitive!(
            Float64Builder,
            match value {
                AvroValue::Float(value) => Some(f64::from(*value)),
                AvroValue::Double(value) => Some(*value),
                _ => None,
            }
        ),
        ParseAsType::Array { element, len } => {
            let AvroValue::Array(values) = value else {
                return Err(incompatible());
            };
            if values.len() != len.get().arch_into() {
                return Err(incompatible());
            }
            let builder = typed_arrow_builder::<FixedSizeListBuilder<Box<dyn ArrayBuilder>>>(
                builder, context,
            )?;
            for (index, value) in values.iter().enumerate() {
                if let Err(error) = append_avro_value_to_arrow(
                    builder.values().as_mut(),
                    element,
                    value,
                    &format!("{context}[{index}]"),
                ) {
                    close_partial_fixed_size_list(builder, element, len.get().arch_into());
                    return Err(error);
                }
            }
            builder.append(true);
            Ok(())
        }
        ParseAsType::Vec { element } => {
            let AvroValue::Array(values) = value else {
                return Err(incompatible());
            };
            let builder =
                typed_arrow_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(builder, context)?;
            for (index, value) in values.iter().enumerate() {
                append_avro_value_to_arrow(
                    builder.values().as_mut(),
                    element,
                    value,
                    &format!("{context}[{index}]"),
                )?;
            }
            builder.append(true);
            Ok(())
        }
    }
}

fn typed_arrow_builder<'a, T: 'static>(
    builder: &'a mut dyn ArrayBuilder,
    context: &str,
) -> Result<&'a mut T, String> {
    builder
        .as_any_mut()
        .downcast_mut::<T>()
        .ok_or_else(|| format!("{context} has an incompatible Arrow builder"))
}

fn avro_value_payload(value: &AvroValue) -> &AvroValue {
    match value {
        AvroValue::Union(_, value) => value.as_ref(),
        other => other,
    }
}

fn avro_value_is_null(value: &AvroValue) -> bool {
    matches!(avro_value_payload(value), AvroValue::Null)
}

pub(crate) fn arrow_data_type(ty: &ParseAsType) -> ArrowDataType {
    match ty {
        ParseAsType::U8 => ArrowDataType::UInt8,
        ParseAsType::I8 => ArrowDataType::Int8,
        ParseAsType::U16 => ArrowDataType::UInt16,
        ParseAsType::I16 => ArrowDataType::Int16,
        ParseAsType::U32 => ArrowDataType::UInt32,
        ParseAsType::I32 => ArrowDataType::Int32,
        ParseAsType::U64 => ArrowDataType::UInt64,
        ParseAsType::I64 => ArrowDataType::Int64,
        ParseAsType::Bool => ArrowDataType::Boolean,
        ParseAsType::String => ArrowDataType::Utf8,
        ParseAsType::Datetime => {
            ArrowDataType::Timestamp(ArrowTimeUnit::Nanosecond, Some("+00:00".into()))
        }
        ParseAsType::F32 => ArrowDataType::Float32,
        ParseAsType::F64 => ArrowDataType::Float64,
        ParseAsType::Array { element, len } => ArrowDataType::FixedSizeList(
            ArrowFieldRef::new(ArrowField::new("item", arrow_data_type(element), false)),
            i32::try_from(len.get()).verified(
                "the schema parser rejects an array length that does not fit an Arrow fixed-size \
                 list",
            ),
        ),
        ParseAsType::Vec { element } => ArrowDataType::List(ArrowFieldRef::new(ArrowField::new(
            "item",
            arrow_data_type(element),
            false,
        ))),
    }
}

/// Closes the fixed-size list value an element append failed part-way through.
///
/// A fixed-size list builder demands `len` child values for every value it holds, so the elements
/// the failed append never wrote are filled in here. They are filled with throwaway values rather
/// than nulls because a list column may not carry a null element, and the row this value belongs
/// to is dropped by [`RuntimeRecordBatchBuilder::abandon_row`] before the batch is built.
fn close_partial_fixed_size_list(
    builder: &mut FixedSizeListBuilder<Box<dyn ArrayBuilder>>,
    element: &ParseAsType,
    len: usize,
) {
    // Every value the builder has closed holds `len` child values, so what is left over is exactly
    // what the failed value wrote.
    let closed = builder
        .len()
        .checked_mul(len)
        .assured("a fixed-size list holds one value per batch row, of a length the schema fixes");
    let written =
        builder.values().len().checked_sub(closed).assured(
            "a fixed-size list builder holds `len` child values for every value it closed",
        );
    let placeholder = placeholder_value(element);
    for _ in written..len {
        append_runtime_value_to_arrow(
            builder.values().as_mut(),
            element,
            Some(&placeholder),
            "abandoned fixed-size list value",
        )
        .assured("a placeholder of the element's own type fits the builder made from that type");
    }
    builder.append(true);
}

/// A throwaway value of `ty`, used to complete a value a failed append left half-written.
fn placeholder_value(ty: &ParseAsType) -> RuntimeValue {
    match ty {
        ParseAsType::U8 => RuntimeValue::U8(0),
        ParseAsType::I8 => RuntimeValue::I8(0),
        ParseAsType::U16 => RuntimeValue::U16(0),
        ParseAsType::I16 => RuntimeValue::I16(0),
        ParseAsType::U32 => RuntimeValue::U32(0),
        ParseAsType::I32 => RuntimeValue::I32(0),
        ParseAsType::U64 => RuntimeValue::U64(0),
        ParseAsType::I64 => RuntimeValue::I64(0),
        ParseAsType::Bool => RuntimeValue::Bool(false),
        ParseAsType::String => RuntimeValue::String(String::new()),
        ParseAsType::Datetime => {
            RuntimeValue::Datetime(DateTime::<chrono::Utc>::UNIX_EPOCH.fixed_offset())
        }
        ParseAsType::F32 => RuntimeValue::F32(OrderedFloat(0.0)),
        ParseAsType::F64 => RuntimeValue::F64(OrderedFloat(0.0)),
        ParseAsType::Array { element, len } => {
            RuntimeValue::Array(vec![placeholder_value(element); len.get().arch_into()])
        }
        ParseAsType::Vec { .. } => RuntimeValue::Vec(Vec::new()),
    }
}

fn append_runtime_value_to_arrow(
    builder: &mut dyn ArrayBuilder,
    ty: &ParseAsType,
    value: Option<&RuntimeValue>,
    context: &str,
) -> Result<(), String> {
    macro_rules! append_primitive {
        ($builder:ty, $variant:path, $map:expr) => {{
            let builder = builder
                .as_any_mut()
                .downcast_mut::<$builder>()
                .ok_or_else(|| format!("{context} has an incompatible Arrow builder"))?;
            match value {
                Some($variant(value)) => {
                    builder.append_value($map(value).ok_or_else(|| {
                        format!("{context} value cannot be represented as {ty:?}")
                    })?)
                }
                None => builder.append_null(),
                Some(value) => {
                    return Err(format!(
                        "{context} expected {ty:?}, got {}",
                        runtime_value_type_name(value)
                    ));
                }
            }
            Ok(())
        }};
    }

    match ty {
        ParseAsType::U8 => {
            append_primitive!(UInt8Builder, RuntimeValue::U8, |value: &u8| Some(*value))
        }
        ParseAsType::I8 => {
            append_primitive!(Int8Builder, RuntimeValue::I8, |value: &i8| Some(*value))
        }
        ParseAsType::U16 => {
            append_primitive!(UInt16Builder, RuntimeValue::U16, |value: &u16| Some(*value))
        }
        ParseAsType::I16 => {
            append_primitive!(Int16Builder, RuntimeValue::I16, |value: &i16| Some(*value))
        }
        ParseAsType::U32 => {
            append_primitive!(UInt32Builder, RuntimeValue::U32, |value: &u32| Some(*value))
        }
        ParseAsType::I32 => {
            append_primitive!(Int32Builder, RuntimeValue::I32, |value: &i32| Some(*value))
        }
        ParseAsType::U64 => {
            append_primitive!(UInt64Builder, RuntimeValue::U64, |value: &u64| Some(*value))
        }
        ParseAsType::I64 => {
            append_primitive!(Int64Builder, RuntimeValue::I64, |value: &i64| Some(*value))
        }
        ParseAsType::Bool => {
            append_primitive!(BooleanBuilder, RuntimeValue::Bool, |value: &bool| Some(
                *value
            ))
        }
        ParseAsType::String => {
            append_primitive!(StringBuilder, RuntimeValue::String, |value: &String| Some(
                value.clone()
            ))
        }
        ParseAsType::Datetime => append_primitive!(
            TimestampNanosecondBuilder,
            RuntimeValue::Datetime,
            |value: &DateTime<FixedOffset>| value.timestamp_nanos_opt()
        ),
        ParseAsType::F32 => {
            append_primitive!(Float32Builder, RuntimeValue::F32, |value: &OrderedFloat<
                f32,
            >| Some(
                value.into_inner()
            ))
        }
        ParseAsType::F64 => {
            append_primitive!(Float64Builder, RuntimeValue::F64, |value: &OrderedFloat<
                f64,
            >| Some(
                value.into_inner()
            ))
        }
        ParseAsType::Array { element, len } => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<FixedSizeListBuilder<Box<dyn ArrayBuilder>>>()
                .ok_or_else(|| format!("{context} has an incompatible Arrow array builder"))?;
            let values = match value {
                Some(RuntimeValue::Array(values)) if values.len() == len.get().arch_into() => {
                    Some(values)
                }
                Some(RuntimeValue::Array(values)) => {
                    return Err(format!(
                        "{context} expected array length {len}, got {}",
                        values.len()
                    ));
                }
                None => None,
                Some(value) => {
                    return Err(format!(
                        "{context} expected ARRAY, got {}",
                        runtime_value_type_name(value)
                    ));
                }
            };
            for index in 0..len.get().arch_into() {
                if let Err(error) = append_runtime_value_to_arrow(
                    builder.values().as_mut(),
                    element,
                    values.map(|values| &values[index]),
                    &format!("{context}[{index}]"),
                ) {
                    close_partial_fixed_size_list(builder, element, len.get().arch_into());
                    return Err(error);
                }
            }
            builder.append(values.is_some());
            Ok(())
        }
        ParseAsType::Vec { element } => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
                .ok_or_else(|| format!("{context} has an incompatible Arrow vector builder"))?;
            let values = match value {
                Some(RuntimeValue::Vec(values)) => Some(values),
                None => None,
                Some(value) => {
                    return Err(format!(
                        "{context} expected VEC, got {}",
                        runtime_value_type_name(value)
                    ));
                }
            };
            if let Some(values) = values {
                for (index, value) in values.iter().enumerate() {
                    append_runtime_value_to_arrow(
                        builder.values().as_mut(),
                        element,
                        Some(value),
                        &format!("{context}[{index}]"),
                    )?;
                }
            }
            builder.append(values.is_some());
            Ok(())
        }
    }
}

fn runtime_value_type_name(value: &RuntimeValue) -> &'static str {
    match value {
        RuntimeValue::U8(_) => "U8",
        RuntimeValue::I8(_) => "I8",
        RuntimeValue::U16(_) => "U16",
        RuntimeValue::I16(_) => "I16",
        RuntimeValue::U32(_) => "U32",
        RuntimeValue::I32(_) => "I32",
        RuntimeValue::U64(_) => "U64",
        RuntimeValue::I64(_) => "I64",
        RuntimeValue::Bool(_) => "BOOL",
        RuntimeValue::String(_) => "STRING",
        RuntimeValue::Datetime(_) => "DATETIME",
        RuntimeValue::F32(_) => "F32",
        RuntimeValue::F64(_) => "F64",
        RuntimeValue::Array(_) => "ARRAY",
        RuntimeValue::Vec(_) => "VEC",
    }
}

/// One Arrow column read as runtime values, with the type derived from the column itself.
///
/// Deriving the type here rather than accepting one beside the array is what keeps a column from
/// being read through a type that describes different bytes.
#[derive(Debug, Clone)]
pub(crate) struct RuntimeValueColumn {
    field: String,
    array: ArrayRef,
    ty: ParseAsType,
}

impl RuntimeValueColumn {
    /// Reads `array` under the name `field`, which names the column in the errors reading it
    /// raises.
    pub(crate) fn new(
        field: impl Into<String>,
        array: ArrayRef,
    ) -> Result<Self, Report<ArrowTypeError>> {
        let ty = parse_as_type_from_arrow(array.data_type())?;
        Ok(Self {
            field: field.into(),
            array,
            ty,
        })
    }

    /// The value at `row`, or `None` where the column is null there.
    pub(crate) fn nullable_value_at(&self, row: usize) -> Result<Option<RuntimeValue>, String> {
        runtime_value_from_arrow_array(self.array.as_ref(), &self.ty, true, row, &self.field)
    }
}

pub(crate) fn runtime_value_from_arrow_array(
    array: &dyn Array,
    ty: &ParseAsType,
    optional: bool,
    row_index: usize,
    field: &str,
) -> Result<Option<RuntimeValue>, String> {
    if array.is_null(row_index) {
        return if optional {
            Ok(None)
        } else {
            Err(format!(
                "arrow batch field '{field}' contains null at row {row_index}"
            ))
        };
    }

    match ty {
        ParseAsType::U8 => Ok(Some(RuntimeValue::U8(
            array
                .as_any()
                .downcast_ref::<UInt8Array>()
                .ok_or_else(|| format!("field '{field}' is not a UInt8Array"))?
                .value(row_index),
        ))),
        ParseAsType::I8 => Ok(Some(RuntimeValue::I8(
            array
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| format!("field '{field}' is not an Int8Array"))?
                .value(row_index),
        ))),
        ParseAsType::U16 => Ok(Some(RuntimeValue::U16(
            array
                .as_any()
                .downcast_ref::<UInt16Array>()
                .ok_or_else(|| format!("field '{field}' is not a UInt16Array"))?
                .value(row_index),
        ))),
        ParseAsType::I16 => Ok(Some(RuntimeValue::I16(
            array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| format!("field '{field}' is not an Int16Array"))?
                .value(row_index),
        ))),
        ParseAsType::U32 => Ok(Some(RuntimeValue::U32(
            array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| format!("field '{field}' is not a UInt32Array"))?
                .value(row_index),
        ))),
        ParseAsType::I32 => Ok(Some(RuntimeValue::I32(
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| format!("field '{field}' is not an Int32Array"))?
                .value(row_index),
        ))),
        ParseAsType::U64 => Ok(Some(RuntimeValue::U64(
            array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| format!("field '{field}' is not a UInt64Array"))?
                .value(row_index),
        ))),
        ParseAsType::I64 => Ok(Some(RuntimeValue::I64(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| format!("field '{field}' is not an Int64Array"))?
                .value(row_index),
        ))),
        ParseAsType::Bool => Ok(Some(RuntimeValue::Bool(
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| format!("field '{field}' is not a BooleanArray"))?
                .value(row_index),
        ))),
        ParseAsType::String => Ok(Some(RuntimeValue::String(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| format!("field '{field}' is not a StringArray"))?
                .value(row_index)
                .to_string(),
        ))),
        ParseAsType::Datetime => Ok(Some(RuntimeValue::Datetime(
            DateTime::from_timestamp_nanos(
                array
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .ok_or_else(|| format!("field '{field}' is not a TimestampNanosecondArray"))?
                    .value(row_index),
            )
            .fixed_offset(),
        ))),
        ParseAsType::F32 => Ok(Some(RuntimeValue::F32(OrderedFloat(
            array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| format!("field '{field}' is not a Float32Array"))?
                .value(row_index),
        )))),
        ParseAsType::F64 => Ok(Some(RuntimeValue::F64(OrderedFloat(
            array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| format!("field '{field}' is not a Float64Array"))?
                .value(row_index),
        )))),
        ParseAsType::Vec { element } => {
            let array = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| format!("field '{field}' is not a ListArray"))?;
            let values = array.value(row_index);
            let values = runtime_values_from_arrow_slice(values.as_ref(), element, field)?;
            Ok(Some(RuntimeValue::Vec(values)))
        }
        ParseAsType::Array { element, len } => {
            let array = array
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or_else(|| format!("field '{field}' is not a FixedSizeListArray"))?;
            if array.value_length() != i32::try_from(len.get()).unwrap_or(i32::MAX) {
                return Err(format!(
                    "field '{field}' fixed-size list length {} does not match schema length {}",
                    array.value_length(),
                    len
                ));
            }
            let values = array.value(row_index);
            let values = runtime_values_from_arrow_slice(values.as_ref(), element, field)?;
            Ok(Some(RuntimeValue::Array(values)))
        }
    }
}

fn runtime_values_from_arrow_slice(
    array: &dyn Array,
    element: &ParseAsType,
    field: &str,
) -> Result<Vec<RuntimeValue>, String> {
    (0..array.len())
        .map(|index| {
            runtime_value_from_arrow_array(array, element, true, index, field)?
                .ok_or_else(|| format!("field '{field}' list contains null at index {index}"))
        })
        .collect()
}

fn json_value_matches_wire_type(value: &JsonValue, ty: JsonType) -> bool {
    match ty {
        JsonType::String => value.is_string(),
        JsonType::Number => value.is_number(),
        JsonType::Integer => value.as_i64().is_some() || value.as_u64().is_some(),
        JsonType::Object => value.is_object(),
        JsonType::Array => value.is_array(),
        JsonType::Boolean => value.is_boolean(),
        JsonType::Null => value.is_null(),
        JsonType::U8 => value.as_u64().and_then(|v| u8::try_from(v).ok()).is_some(),
        JsonType::I8 => value.as_i64().and_then(|v| i8::try_from(v).ok()).is_some(),
        JsonType::U16 => value.as_u64().and_then(|v| u16::try_from(v).ok()).is_some(),
        JsonType::I16 => value.as_i64().and_then(|v| i16::try_from(v).ok()).is_some(),
        JsonType::U32 => value.as_u64().and_then(|v| u32::try_from(v).ok()).is_some(),
        JsonType::I32 => value.as_i64().and_then(|v| i32::try_from(v).ok()).is_some(),
        JsonType::U64 => value.as_u64().is_some(),
        JsonType::I64 => value.as_i64().is_some(),
        JsonType::Datetime => value
            .as_str()
            .and_then(|v| DateTime::parse_from_rfc3339(v).ok())
            .is_some(),
        JsonType::F32 | JsonType::F64 => value.is_number(),
    }
}

fn avro_to_i64(value: &AvroValue) -> Option<i64> {
    match value {
        AvroValue::Int(v) => Some(i64::from(*v)),
        AvroValue::Long(v) => Some(*v),
        _ => None,
    }
}

fn avro_to_u64(value: &AvroValue) -> Option<u64> {
    avro_to_i64(value).and_then(|v| u64::try_from(v).ok())
}

fn avro_schema_json(
    schema: &CreateWireSchema<AvroType>,
    internal_fields: &[CompiledSchemaField],
) -> String {
    let fields = schema
        .fields
        .iter()
        .map(|field| avro_wire_field_json(field, internal_fields))
        .collect::<Vec<_>>()
        .join(",");

    format!(
        r#"{{"type":"record","name":"{}","fields":[{}]}}"#,
        schema.name.as_str(),
        fields
    )
}

fn avro_wire_field_json(
    field: &WireSchemaField<AvroType>,
    internal_fields: &[CompiledSchemaField],
) -> String {
    let ty = avro_type_json(field, internal_fields);
    if field.optional {
        format!(
            r#"{{"name":"{}","type":["null",{}],"default":null}}"#,
            field.name.as_str(),
            ty
        )
    } else {
        format!(r#"{{"name":"{}","type":{}}}"#, field.name.as_str(), ty)
    }
}

fn avro_type_json(
    field: &WireSchemaField<AvroType>,
    internal_fields: &[CompiledSchemaField],
) -> String {
    if let AvroType::Array = field.ty
        && let Some(internal) = internal_fields
            .iter()
            .find(|internal| internal.name == field.name.as_str())
        && let ParseAsType::Array { .. } | ParseAsType::Vec { .. } = &internal.ty
    {
        return parse_as_avro_type_json(&internal.ty);
    }
    format!(r#""{}""#, avro_type_name(field.ty))
}

fn parse_as_avro_type_json(ty: &ParseAsType) -> String {
    match ty {
        ParseAsType::Bool => r#""boolean""#.to_string(),
        ParseAsType::U8
        | ParseAsType::I8
        | ParseAsType::U16
        | ParseAsType::I16
        | ParseAsType::U32
        | ParseAsType::I32
        | ParseAsType::U64
        | ParseAsType::I64 => r#""long""#.to_string(),
        ParseAsType::F32 => r#""float""#.to_string(),
        ParseAsType::F64 => r#""double""#.to_string(),
        ParseAsType::String | ParseAsType::Datetime => r#""string""#.to_string(),
        ParseAsType::Array { element, .. } | ParseAsType::Vec { element } => format!(
            r#"{{"type":"array","items":{}}}"#,
            parse_as_avro_type_json(element)
        ),
    }
}

fn avro_type_name(ty: AvroType) -> &'static str {
    match ty {
        AvroType::Null => "null",
        AvroType::Boolean => "boolean",
        AvroType::Int => "int",
        AvroType::Long => "long",
        AvroType::Float => "float",
        AvroType::Double => "double",
        AvroType::Bytes => "bytes",
        AvroType::String => "string",
        AvroType::Record => "record",
        AvroType::Enum => "enum",
        AvroType::Array => "array",
        AvroType::Map => "map",
        AvroType::Fixed => "fixed",
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Datelike, Utc};
    use nervix_models::{
        CodecJaqFormat, CodecJaqTransformations, CodecProtobufConfig, CodecWireFormat,
        CreateAvroWireSchema, CreateCborWireSchema, CreateCodec, CreateJsonWireSchema,
        CreateSchema, CreateWireSchema, SchemaField, WireSchemaLookup, WireSchemaName,
    };
    use nonzero_ext::nonzero;
    use rstest::{fixture, rstest};

    use super::*;

    fn named<N>(raw: &str) -> N
    where
        N: for<'a> TryFrom<&'a str>,
        for<'a> <N as TryFrom<&'a str>>::Error: std::fmt::Debug,
    {
        N::try_from(raw).expect("valid name")
    }

    fn schema() -> CreateSchema {
        CreateSchema {
            name: named("notification"),
            fields: vec![
                SchemaField {
                    name: named("user_id"),
                    ty: ParseAsType::U32,
                    optional: false,
                    sensitive: false,
                },
                SchemaField {
                    name: named("tenant"),
                    ty: ParseAsType::String,
                    optional: false,
                    sensitive: false,
                },
                SchemaField {
                    name: named("created_at"),
                    ty: ParseAsType::Datetime,
                    optional: false,
                    sensitive: false,
                },
                SchemaField {
                    name: named("latency"),
                    ty: ParseAsType::F64,
                    optional: false,
                    sensitive: false,
                },
                SchemaField {
                    name: named("active"),
                    ty: ParseAsType::Bool,
                    optional: false,
                    sensitive: false,
                },
            ],
        }
    }

    fn json_wire_schema() -> CreateJsonWireSchema {
        CreateWireSchema {
            name: named("notification_wire"),
            strictness: Default::default(),
            fields: vec![
                WireSchemaField {
                    name: named("user_id"),
                    ty: JsonType::Integer,
                    optional: false,
                },
                WireSchemaField {
                    name: named("tenant"),
                    ty: JsonType::String,
                    optional: false,
                },
                WireSchemaField {
                    name: named("created_at"),
                    ty: JsonType::String,
                    optional: false,
                },
                WireSchemaField {
                    name: named("latency"),
                    ty: JsonType::Number,
                    optional: false,
                },
                WireSchemaField {
                    name: named("active"),
                    ty: JsonType::Boolean,
                    optional: false,
                },
            ],
        }
    }

    fn json_wire_schema_with_strictness(strictness: WireSchemaStrictness) -> CreateJsonWireSchema {
        let mut wire_schema = json_wire_schema();
        wire_schema.strictness = strictness;
        wire_schema
    }

    fn cbor_wire_schema(strictness: WireSchemaStrictness) -> CreateCborWireSchema {
        CreateWireSchema {
            name: named("notification_wire"),
            strictness,
            fields: vec![
                WireSchemaField {
                    name: named("user_id"),
                    ty: JsonType::Integer,
                    optional: false,
                },
                WireSchemaField {
                    name: named("tenant"),
                    ty: JsonType::String,
                    optional: false,
                },
                WireSchemaField {
                    name: named("created_at"),
                    ty: JsonType::String,
                    optional: false,
                },
                WireSchemaField {
                    name: named("latency"),
                    ty: JsonType::Number,
                    optional: false,
                },
                WireSchemaField {
                    name: named("active"),
                    ty: JsonType::Boolean,
                    optional: false,
                },
            ],
        }
    }

    fn avro_wire_schema() -> CreateAvroWireSchema {
        CreateWireSchema {
            name: named("notification_avro"),
            strictness: Default::default(),
            fields: vec![
                WireSchemaField {
                    name: named("user_id"),
                    ty: AvroType::Long,
                    optional: false,
                },
                WireSchemaField {
                    name: named("tenant"),
                    ty: AvroType::String,
                    optional: false,
                },
                WireSchemaField {
                    name: named("created_at"),
                    ty: AvroType::String,
                    optional: false,
                },
                WireSchemaField {
                    name: named("latency"),
                    ty: AvroType::Double,
                    optional: false,
                },
                WireSchemaField {
                    name: named("active"),
                    ty: AvroType::Boolean,
                    optional: false,
                },
            ],
        }
    }

    fn optional_schema() -> CreateSchema {
        CreateSchema {
            name: named("optional_notification"),
            fields: vec![
                SchemaField {
                    name: named("user_id"),
                    ty: ParseAsType::U32,
                    optional: false,
                    sensitive: false,
                },
                SchemaField {
                    name: named("nickname"),
                    ty: ParseAsType::String,
                    optional: true,
                    sensitive: false,
                },
            ],
        }
    }

    fn optional_json_wire_schema() -> CreateJsonWireSchema {
        CreateWireSchema {
            name: named("optional_notification_wire"),
            strictness: Default::default(),
            fields: vec![
                WireSchemaField {
                    name: named("user_id"),
                    ty: JsonType::Integer,
                    optional: false,
                },
                WireSchemaField {
                    name: named("nickname"),
                    ty: JsonType::String,
                    optional: true,
                },
            ],
        }
    }

    fn optional_avro_wire_schema() -> CreateAvroWireSchema {
        CreateWireSchema {
            name: named("optional_notification_avro"),
            strictness: Default::default(),
            fields: vec![
                WireSchemaField {
                    name: named("user_id"),
                    ty: AvroType::Long,
                    optional: false,
                },
                WireSchemaField {
                    name: named("nickname"),
                    ty: AvroType::String,
                    optional: true,
                },
            ],
        }
    }

    fn array_schema() -> CreateSchema {
        CreateSchema {
            name: named("metrics"),
            fields: vec![
                SchemaField {
                    name: named("cpu_last_64"),
                    ty: ParseAsType::Array {
                        element: Box::new(ParseAsType::F32),
                        len: nonzero!(3u32),
                    },
                    optional: false,
                    sensitive: false,
                },
                SchemaField {
                    name: named("labels"),
                    ty: ParseAsType::Vec {
                        element: Box::new(ParseAsType::String),
                    },
                    optional: true,
                    sensitive: false,
                },
            ],
        }
    }

    fn array_json_wire_schema() -> CreateJsonWireSchema {
        CreateWireSchema {
            name: named("metrics_json"),
            strictness: Default::default(),
            fields: vec![
                WireSchemaField {
                    name: named("cpu_last_64"),
                    ty: JsonType::Array,
                    optional: false,
                },
                WireSchemaField {
                    name: named("labels"),
                    ty: JsonType::Array,
                    optional: true,
                },
            ],
        }
    }

    fn array_avro_wire_schema() -> CreateAvroWireSchema {
        CreateWireSchema {
            name: named("metrics_avro"),
            strictness: Default::default(),
            fields: vec![
                WireSchemaField {
                    name: named("cpu_last_64"),
                    ty: AvroType::Array,
                    optional: false,
                },
                WireSchemaField {
                    name: named("labels"),
                    ty: AvroType::Array,
                    optional: true,
                },
            ],
        }
    }

    fn array_codec(name: &str) -> CreateCodec {
        CreateCodec {
            name: named(name),
            wire_format: if name.contains("avro") {
                CodecWireFormat::Avro {
                    wire_schema: named("metrics_wire"),
                }
            } else {
                CodecWireFormat::Json {
                    wire_schema: named("metrics_wire"),
                }
            },
            schema: named("metrics"),
            encoding_rules: Vec::new(),
        }
    }

    fn array_record() -> RuntimeRow {
        test_runtime_row([
            (
                "cpu_last_64".to_string(),
                RuntimeValue::Array(vec![
                    RuntimeValue::F32(OrderedFloat(1.0)),
                    RuntimeValue::F32(OrderedFloat(2.5)),
                    RuntimeValue::F32(OrderedFloat(3.25)),
                ]),
            ),
            (
                "labels".to_string(),
                RuntimeValue::Vec(vec![
                    RuntimeValue::String("prod".to_string()),
                    RuntimeValue::String("api".to_string()),
                ]),
            ),
        ])
    }

    fn multidimensional_array_schema() -> CreateSchema {
        CreateSchema {
            name: named("shaped_metrics"),
            fields: vec![
                SchemaField {
                    name: named("matrix"),
                    ty: ParseAsType::Array {
                        len: nonzero!(2u32),
                        element: Box::new(ParseAsType::Array {
                            len: nonzero!(3u32),
                            element: Box::new(ParseAsType::F32),
                        }),
                    },
                    optional: false,
                    sensitive: false,
                },
                SchemaField {
                    name: named("samples"),
                    ty: ParseAsType::Vec {
                        element: Box::new(ParseAsType::Array {
                            len: nonzero!(2u32),
                            element: Box::new(ParseAsType::F32),
                        }),
                    },
                    optional: false,
                    sensitive: false,
                },
            ],
        }
    }

    fn multidimensional_array_record() -> RuntimeRow {
        let f32_value = |value| RuntimeValue::F32(OrderedFloat(value));
        test_runtime_row([
            (
                "matrix".to_string(),
                RuntimeValue::Array(vec![
                    RuntimeValue::Array(vec![f32_value(1.0), f32_value(2.0), f32_value(3.0)]),
                    RuntimeValue::Array(vec![f32_value(4.0), f32_value(5.0), f32_value(6.0)]),
                ]),
            ),
            (
                "samples".to_string(),
                RuntimeValue::Vec(vec![
                    RuntimeValue::Array(vec![f32_value(10.0), f32_value(11.0)]),
                    RuntimeValue::Array(vec![f32_value(20.0), f32_value(21.0)]),
                    RuntimeValue::Array(vec![f32_value(30.0), f32_value(31.0)]),
                ]),
            ),
        ])
    }

    fn multidimensional_avro_wire_schema() -> CreateAvroWireSchema {
        CreateWireSchema {
            name: named("shaped_metrics_wire"),
            strictness: Default::default(),
            fields: ["matrix", "samples"]
                .into_iter()
                .map(|name| WireSchemaField {
                    name: named(name),
                    ty: AvroType::Array,
                    optional: false,
                })
                .collect(),
        }
    }

    /// One primitive element type covered by the array and vector round-trip tests: the field
    /// name prefix it uses, its declared type, the runtime values it carries, and the JSON those
    /// values encode to.
    struct PrimitiveArrayCase {
        name: &'static str,
        ty: ParseAsType,
        values: Vec<RuntimeValue>,
        json: Vec<JsonValue>,
    }

    fn primitive_array_cases() -> Vec<PrimitiveArrayCase> {
        vec![
            PrimitiveArrayCase {
                name: "u8",
                ty: ParseAsType::U8,
                values: vec![RuntimeValue::U8(1), RuntimeValue::U8(2)],
                json: vec![JsonValue::from(1), JsonValue::from(2)],
            },
            PrimitiveArrayCase {
                name: "i8",
                ty: ParseAsType::I8,
                values: vec![RuntimeValue::I8(-1), RuntimeValue::I8(2)],
                json: vec![JsonValue::from(-1), JsonValue::from(2)],
            },
            PrimitiveArrayCase {
                name: "u16",
                ty: ParseAsType::U16,
                values: vec![RuntimeValue::U16(10), RuntimeValue::U16(20)],
                json: vec![JsonValue::from(10), JsonValue::from(20)],
            },
            PrimitiveArrayCase {
                name: "i16",
                ty: ParseAsType::I16,
                values: vec![RuntimeValue::I16(-10), RuntimeValue::I16(20)],
                json: vec![JsonValue::from(-10), JsonValue::from(20)],
            },
            PrimitiveArrayCase {
                name: "u32",
                ty: ParseAsType::U32,
                values: vec![RuntimeValue::U32(100), RuntimeValue::U32(200)],
                json: vec![JsonValue::from(100), JsonValue::from(200)],
            },
            PrimitiveArrayCase {
                name: "i32",
                ty: ParseAsType::I32,
                values: vec![RuntimeValue::I32(-100), RuntimeValue::I32(200)],
                json: vec![JsonValue::from(-100), JsonValue::from(200)],
            },
            PrimitiveArrayCase {
                name: "u64",
                ty: ParseAsType::U64,
                values: vec![RuntimeValue::U64(1000), RuntimeValue::U64(2000)],
                json: vec![JsonValue::from(1000), JsonValue::from(2000)],
            },
            PrimitiveArrayCase {
                name: "i64",
                ty: ParseAsType::I64,
                values: vec![RuntimeValue::I64(-1000), RuntimeValue::I64(2000)],
                json: vec![JsonValue::from(-1000), JsonValue::from(2000)],
            },
            PrimitiveArrayCase {
                name: "bool",
                ty: ParseAsType::Bool,
                values: vec![RuntimeValue::Bool(true), RuntimeValue::Bool(false)],
                json: vec![JsonValue::from(true), JsonValue::from(false)],
            },
            PrimitiveArrayCase {
                name: "string",
                ty: ParseAsType::String,
                values: vec![
                    RuntimeValue::String("prod".to_string()),
                    RuntimeValue::String("api".to_string()),
                ],
                json: vec![JsonValue::from("prod"), JsonValue::from("api")],
            },
            PrimitiveArrayCase {
                name: "datetime",
                ty: ParseAsType::Datetime,
                values: vec![
                    RuntimeValue::Datetime(
                        DateTime::parse_from_rfc3339("2025-01-02T03:04:05Z")
                            .expect("valid timestamp"),
                    ),
                    RuntimeValue::Datetime(
                        DateTime::parse_from_rfc3339("2025-01-02T03:04:06Z")
                            .expect("valid timestamp"),
                    ),
                ],
                json: vec![
                    JsonValue::from("2025-01-02T03:04:05Z"),
                    JsonValue::from("2025-01-02T03:04:06Z"),
                ],
            },
            PrimitiveArrayCase {
                name: "f32",
                ty: ParseAsType::F32,
                values: vec![
                    RuntimeValue::F32(OrderedFloat(1.25)),
                    RuntimeValue::F32(OrderedFloat(2.5)),
                ],
                json: vec![JsonValue::from(1.25), JsonValue::from(2.5)],
            },
            PrimitiveArrayCase {
                name: "f64",
                ty: ParseAsType::F64,
                values: vec![
                    RuntimeValue::F64(OrderedFloat(10.25)),
                    RuntimeValue::F64(OrderedFloat(20.5)),
                ],
                json: vec![JsonValue::from(10.25), JsonValue::from(20.5)],
            },
        ]
    }

    fn primitive_arrays_schema() -> CreateSchema {
        let mut fields = Vec::new();
        for case in primitive_array_cases() {
            let name = case.name;
            fields.push(SchemaField {
                name: named(&format!("{name}_array")),
                ty: ParseAsType::Array {
                    element: Box::new(case.ty.clone()),
                    len: nonzero!(2u32),
                },
                optional: false,
                sensitive: false,
            });
            fields.push(SchemaField {
                name: named(&format!("{name}_vec")),
                ty: ParseAsType::Vec {
                    element: Box::new(case.ty),
                },
                optional: false,
                sensitive: false,
            });
        }
        CreateSchema {
            name: named("primitive_arrays"),
            fields,
        }
    }

    fn primitive_arrays_json_wire_schema() -> CreateJsonWireSchema {
        CreateWireSchema {
            name: named("primitive_arrays_wire"),
            strictness: Default::default(),
            fields: primitive_arrays_schema()
                .fields
                .iter()
                .map(|field| WireSchemaField {
                    name: field.name.clone(),
                    ty: JsonType::Array,
                    optional: false,
                })
                .collect(),
        }
    }

    fn primitive_arrays_avro_wire_schema() -> CreateAvroWireSchema {
        CreateWireSchema {
            name: named("primitive_arrays_wire"),
            strictness: Default::default(),
            fields: primitive_arrays_schema()
                .fields
                .iter()
                .map(|field| WireSchemaField {
                    name: field.name.clone(),
                    ty: AvroType::Array,
                    optional: false,
                })
                .collect(),
        }
    }

    fn primitive_arrays_codec(name: &str) -> CreateCodec {
        CreateCodec {
            name: named(name),
            wire_format: if name.contains("avro") {
                CodecWireFormat::Avro {
                    wire_schema: named("primitive_arrays_wire"),
                }
            } else {
                CodecWireFormat::Json {
                    wire_schema: named("primitive_arrays_wire"),
                }
            },
            schema: named("primitive_arrays"),
            encoding_rules: Vec::new(),
        }
    }

    fn jaq_native_codec(
        name: &str,
        format: CodecJaqFormat,
        schema: &str,
        on_ingestion: Option<&str>,
        on_emitting: Option<&str>,
    ) -> CreateCodec {
        CreateCodec {
            name: named(name),
            wire_format: CodecWireFormat::JaqNative {
                format,
                transformations: CodecJaqTransformations {
                    on_ingestion: on_ingestion.map(str::to_string),
                    on_emitting: on_emitting.map(str::to_string),
                },
            },
            schema: named(schema),
            encoding_rules: Vec::new(),
        }
    }

    /// A wire schema lookup that holds none, for codec fixtures whose format carries its own
    /// decoding contract and therefore never asks for one.
    struct NoWireSchemas;

    static NO_WIRE_SCHEMAS: NoWireSchemas = NoWireSchemas;

    impl WireSchemaLookup for NoWireSchemas {
        type Error = WireSchemaName;

        fn json_wire_schema(
            &self,
            name: &WireSchemaName,
        ) -> Result<&CreateJsonWireSchema, Self::Error> {
            Err(name.clone())
        }

        fn cbor_wire_schema(
            &self,
            name: &WireSchemaName,
        ) -> Result<&CreateCborWireSchema, Self::Error> {
            Err(name.clone())
        }

        fn avro_wire_schema(
            &self,
            name: &WireSchemaName,
        ) -> Result<&CreateAvroWireSchema, Self::Error> {
            Err(name.clone())
        }
    }

    /// The resolved form of a codec fixture whose wire format names no wire schema.
    fn self_describing(wire_format: &CodecWireFormat) -> ResolvedCodecWireFormat<'_> {
        wire_format
            .resolve(&NO_WIRE_SCHEMAS)
            .expect("this fixture's wire format names no wire schema")
    }

    fn jaq_native_identity_codec(name: &str, format: CodecJaqFormat, schema: &str) -> CreateCodec {
        jaq_native_codec(name, format, schema, Some("."), Some("."))
    }

    fn protobuf_schema() -> CreateSchema {
        CreateSchema {
            name: named("protobuf_notification"),
            fields: vec![
                SchemaField {
                    name: named("user_id"),
                    ty: ParseAsType::U32,
                    optional: false,
                    sensitive: false,
                },
                SchemaField {
                    name: named("tenant"),
                    ty: ParseAsType::String,
                    optional: false,
                    sensitive: false,
                },
                SchemaField {
                    name: named("payload"),
                    ty: ParseAsType::String,
                    optional: false,
                    sensitive: false,
                },
            ],
        }
    }

    fn protobuf_codec(
        name: &str,
        on_ingestion: Option<&str>,
        on_emitting: Option<&str>,
    ) -> CreateCodec {
        CreateCodec {
            name: named(name),
            wire_format: CodecWireFormat::Protobuf(CodecProtobufConfig {
                resource: named("proto_bundle"),
                resource_version: Some(1),
                config: vec![nervix_models::ClientConfigEntry {
                    key: "file".to_string(),
                    value: "notification.proto".to_string(),
                }],
                message: "nervix.test.Notification".to_string(),
                transformations: CodecJaqTransformations {
                    on_ingestion: on_ingestion.map(str::to_string),
                    on_emitting: on_emitting.map(str::to_string),
                },
            }),
            schema: named("protobuf_notification"),
            encoding_rules: Vec::new(),
        }
    }

    fn protobuf_descriptor() -> MessageDescriptor {
        let dir = tempfile::tempdir().expect("tempdir should be created");
        let proto_path = dir.path().join("notification.proto");
        std::fs::write(
            &proto_path,
            r#"
                syntax = "proto3";
                package nervix.test;

                message Notification {
                  uint32 user_id = 1;
                  string tenant = 2;
                  string payload = 3;
                }
            "#,
        )
        .expect("proto file should be written");
        let file_descriptor_set =
            protox::compile([proto_path], [dir.path()]).expect("proto should compile");
        ProtobufDescriptorPool::from_file_descriptor_set(file_descriptor_set)
            .expect("descriptor pool should be built")
            .message("nervix.test.Notification")
            .expect("descriptor should be built")
    }

    fn primitive_arrays_record() -> RuntimeRow {
        let mut fields = Vec::new();
        for case in primitive_array_cases() {
            let name = case.name;
            fields.push((
                format!("{name}_array"),
                RuntimeValue::Array(case.values.clone()),
            ));
            fields.push((format!("{name}_vec"), RuntimeValue::Vec(case.values)));
        }
        test_runtime_row(fields)
    }

    fn primitive_arrays_json_payload() -> Vec<u8> {
        let mut object = JsonMap::new();
        for case in primitive_array_cases() {
            let name = case.name;
            object.insert(format!("{name}_array"), JsonValue::Array(case.json.clone()));
            object.insert(format!("{name}_vec"), JsonValue::Array(case.json));
        }
        serde_json::to_vec(&JsonValue::Object(object)).expect("valid json")
    }

    fn optional_codec(name: &str) -> CreateCodec {
        CreateCodec {
            name: named(name),
            wire_format: if name.contains("avro") {
                CodecWireFormat::Avro {
                    wire_schema: named("optional_notification_wire"),
                }
            } else {
                CodecWireFormat::Json {
                    wire_schema: named("optional_notification_wire"),
                }
            },
            schema: named("optional_notification"),
            encoding_rules: Vec::new(),
        }
    }

    fn codec(name: &str) -> CreateCodec {
        CreateCodec {
            name: named(name),
            wire_format: if name.contains("avro") {
                CodecWireFormat::Avro {
                    wire_schema: named("notification_wire"),
                }
            } else {
                CodecWireFormat::Json {
                    wire_schema: named("notification_wire"),
                }
            },
            schema: named("notification"),
            encoding_rules: Vec::new(),
        }
    }

    fn syslog_schema() -> CreateSchema {
        CreateSchema {
            name: named("syslog_event"),
            fields: vec![
                SchemaField {
                    name: named("facility"),
                    ty: ParseAsType::U8,
                    optional: false,
                    sensitive: false,
                },
                SchemaField {
                    name: named("severity"),
                    ty: ParseAsType::U8,
                    optional: false,
                    sensitive: false,
                },
                SchemaField {
                    name: named("timestamp"),
                    ty: ParseAsType::Datetime,
                    optional: true,
                    sensitive: false,
                },
                SchemaField {
                    name: named("hostname"),
                    ty: ParseAsType::String,
                    optional: true,
                    sensitive: false,
                },
                SchemaField {
                    name: named("app_name"),
                    ty: ParseAsType::String,
                    optional: true,
                    sensitive: false,
                },
                SchemaField {
                    name: named("proc_id"),
                    ty: ParseAsType::String,
                    optional: true,
                    sensitive: false,
                },
                SchemaField {
                    name: named("msg_id"),
                    ty: ParseAsType::String,
                    optional: true,
                    sensitive: false,
                },
                SchemaField {
                    name: named("structured_data"),
                    ty: ParseAsType::String,
                    optional: true,
                    sensitive: false,
                },
                SchemaField {
                    name: named("message"),
                    ty: ParseAsType::String,
                    optional: false,
                    sensitive: false,
                },
            ],
        }
    }

    fn syslog_codec() -> CreateCodec {
        CreateCodec {
            name: named("syslog_codec"),
            wire_format: CodecWireFormat::Syslog,
            schema: named("syslog_event"),
            encoding_rules: Vec::new(),
        }
    }

    fn compiled_syslog_codec() -> Arc<CompiledCodec> {
        let codec = syslog_codec();
        compile_codec(
            &codec,
            Arc::new(compile_schema(&syslog_schema())),
            self_describing(&codec.wire_format),
        )
        .expect("SYSLOG codec should compile")
    }

    fn schemaful_cbor_codec(name: &str) -> CreateCodec {
        CreateCodec {
            name: named(name),
            wire_format: CodecWireFormat::Cbor {
                wire_schema: named("notification_wire"),
            },
            schema: named("notification"),
            encoding_rules: Vec::new(),
        }
    }

    fn notification_json_payload_with_extra() -> &'static [u8] {
        br#"{"user_id":42,"tenant":"acme","created_at":"2025-01-02T03:04:05+00:00","latency":12.5,"active":true,"ignored":"drop"}"#
    }

    fn notification_cbor_payload_with_extra() -> Vec<u8> {
        let value: JsonValue = serde_json::from_slice(notification_json_payload_with_extra())
            .expect("fixture should be valid json");
        let mut payload = Vec::new();
        ciborium::into_writer(&value, &mut payload).expect("fixture should encode as cbor");
        payload
    }

    fn record() -> RuntimeRow {
        record_with(42, "acme")
    }

    fn record_with(user_id: u32, tenant: &str) -> RuntimeRow {
        test_runtime_row([
            ("user_id".to_string(), RuntimeValue::U32(user_id)),
            (
                "tenant".to_string(),
                RuntimeValue::String(tenant.to_string()),
            ),
            (
                "created_at".to_string(),
                RuntimeValue::Datetime(
                    DateTime::parse_from_rfc3339("2025-01-02T03:04:05+00:00")
                        .expect("valid timestamp"),
                ),
            ),
            ("latency".to_string(), RuntimeValue::F64(OrderedFloat(12.5))),
            ("active".to_string(), RuntimeValue::Bool(true)),
        ])
    }

    fn concat_test_rows(rows: &[RuntimeRow]) -> Result<RuntimeRecordBatch, String> {
        let batches = rows
            .iter()
            .map(RuntimeRow::one_row_batch)
            .collect::<Vec<_>>();
        RuntimeRecordBatch::concat(&batches.iter().collect::<Vec<_>>())
    }

    /// Decodes one payload into a batch of its own, for a test that asserts on one message.
    fn decode_one(codec: &CompiledCodec, payload: &[u8]) -> Result<RuntimeRecordBatch, CodecError> {
        let mut builder = codec.schema.batch_builder(1);
        decode_with_codec(codec, Cow::Borrowed(payload), &mut builder)?;
        builder.finish().map_err(|reason| CodecError::InvalidCodec {
            codec: codec.name.as_str().to_string(),
            reason,
        })
    }

    fn single_batch_value(batch: &RuntimeRecordBatch, field: &str) -> Option<RuntimeValue> {
        assert_eq!(batch.batch().num_rows(), 1, "expected one Arrow row");
        batch.value(0, field).expect("Arrow value must be readable")
    }

    fn row_value(row: &RuntimeRow, field: &str) -> Option<RuntimeValue> {
        row.value(field).expect("Arrow row value must be readable")
    }

    fn encode_arrow_record(
        codec: &CompiledCodec,
        record: &RuntimeRow,
    ) -> Result<Vec<u8>, CodecError> {
        let batch = record
            .one_row_batch()
            .project(codec.schema.arrow_schema())
            .map_err(|reason| CodecError::InvalidCodec {
                codec: codec.name.as_str().to_string(),
                reason,
            })?;
        let encoder = codec.batch_encoder(&batch)?;
        let mut payload = Vec::new();
        encoder.encode_row_into(0, &mut payload)?;
        Ok(payload)
    }

    fn encode_test_fields(
        codec: &CompiledCodec,
        fields: impl IntoIterator<Item = (String, RuntimeValue)>,
    ) -> Result<Vec<u8>, CodecError> {
        let batch = codec
            .schema
            .batch_from_test_rows([fields])
            .map_err(|reason| CodecError::InvalidCodec {
                codec: codec.name.as_str().to_string(),
                reason,
            })?;
        let encoder = codec.batch_encoder(&batch)?;
        let mut payload = Vec::new();
        encoder.encode_row_into(0, &mut payload)?;
        Ok(payload)
    }

    #[derive(Debug)]
    enum NotificationCodecCase {
        Json,
        Avro,
        SchemafulCbor,
        JaqNativeCbor,
    }

    impl NotificationCodecCase {
        fn compile(&self, schema: Arc<CompiledSchema>) -> Arc<CompiledCodec> {
            match self {
                Self::Json => compile_codec(
                    &codec("json_codec"),
                    schema,
                    ResolvedCodecWireFormat::Json(&json_wire_schema()),
                ),
                Self::Avro => compile_codec(
                    &codec("avro_codec"),
                    schema,
                    ResolvedCodecWireFormat::Avro(&avro_wire_schema()),
                ),
                Self::SchemafulCbor => compile_codec(
                    &schemaful_cbor_codec("schemaful_cbor_codec"),
                    schema,
                    ResolvedCodecWireFormat::Cbor(&cbor_wire_schema(WireSchemaStrictness::Strict)),
                ),
                Self::JaqNativeCbor => {
                    let codec = jaq_native_identity_codec(
                        "cbor_codec",
                        CodecJaqFormat::Cbor,
                        "notification",
                    );
                    let wire_format = self_describing(&codec.wire_format);
                    compile_codec(&codec, schema, wire_format)
                }
            }
            .expect("codec fixture should compile")
        }
    }

    #[derive(Debug)]
    enum ArrayCodecCase {
        Avro,
        Cbor,
        Yaml,
    }

    impl ArrayCodecCase {
        fn compile(&self, schema: Arc<CompiledSchema>) -> Arc<CompiledCodec> {
            match self {
                Self::Avro => compile_codec(
                    &array_codec("avro_array_codec"),
                    schema,
                    ResolvedCodecWireFormat::Avro(&array_avro_wire_schema()),
                ),
                Self::Cbor => {
                    let codec = jaq_native_identity_codec(
                        "cbor_array_codec",
                        CodecJaqFormat::Cbor,
                        "metrics",
                    );
                    let wire_format = self_describing(&codec.wire_format);
                    compile_codec(&codec, schema, wire_format)
                }
                Self::Yaml => {
                    let codec = jaq_native_identity_codec(
                        "yaml_array_codec",
                        CodecJaqFormat::Yaml,
                        "metrics",
                    );
                    let wire_format = self_describing(&codec.wire_format);
                    compile_codec(&codec, schema, wire_format)
                }
            }
            .expect("array codec fixture should compile")
        }
    }

    #[derive(Debug)]
    enum PrimitiveArrayCodecCase {
        Avro,
        Cbor,
        Toml,
    }

    impl PrimitiveArrayCodecCase {
        fn compile(&self, schema: Arc<CompiledSchema>) -> Arc<CompiledCodec> {
            match self {
                Self::Avro => compile_codec(
                    &primitive_arrays_codec("avro_primitive_arrays_codec"),
                    schema,
                    ResolvedCodecWireFormat::Avro(&primitive_arrays_avro_wire_schema()),
                ),
                Self::Cbor => {
                    let codec = jaq_native_identity_codec(
                        "cbor_primitive_arrays_codec",
                        CodecJaqFormat::Cbor,
                        "primitive_arrays",
                    );
                    let wire_format = self_describing(&codec.wire_format);
                    compile_codec(&codec, schema, wire_format)
                }
                Self::Toml => {
                    let codec = jaq_native_identity_codec(
                        "toml_primitive_arrays_codec",
                        CodecJaqFormat::Toml,
                        "primitive_arrays",
                    );
                    let wire_format = self_describing(&codec.wire_format);
                    compile_codec(&codec, schema, wire_format)
                }
            }
            .expect("primitive-array codec fixture should compile")
        }
    }

    #[fixture]
    fn compiled_notification_schema() -> Arc<CompiledSchema> {
        Arc::new(compile_schema(&schema()))
    }

    #[fixture]
    fn notification_record() -> RuntimeRow {
        record()
    }

    #[fixture]
    fn compiled_array_schema() -> Arc<CompiledSchema> {
        Arc::new(compile_schema(&array_schema()))
    }

    #[fixture]
    fn array_record_fixture() -> RuntimeRow {
        array_record()
    }

    #[fixture]
    fn compiled_primitive_array_schema() -> Arc<CompiledSchema> {
        Arc::new(compile_schema(&primitive_arrays_schema()))
    }

    #[fixture]
    fn primitive_array_record_fixture() -> RuntimeRow {
        primitive_arrays_record()
    }

    #[test]
    fn compiled_schema_exposes_arrow_schema() {
        let compiled = compile_schema(&schema());
        let arrow_schema = compiled.arrow_schema();
        assert_eq!(arrow_schema.fields().len(), 5);
        assert_eq!(arrow_schema.field(0).name(), "user_id");
        assert_eq!(arrow_schema.field(0).data_type(), &ArrowDataType::UInt32);
        assert_eq!(
            arrow_schema.field(2).data_type(),
            &ArrowDataType::Timestamp(ArrowTimeUnit::Nanosecond, Some("+00:00".into()))
        );
    }

    #[test]
    fn arrow_values_roundtrip_through_a_batch() {
        let records = vec![
            record(),
            test_runtime_row([
                ("user_id".to_string(), RuntimeValue::U32(7)),
                (
                    "tenant".to_string(),
                    RuntimeValue::String("beta".to_string()),
                ),
                (
                    "created_at".to_string(),
                    RuntimeValue::Datetime(
                        DateTime::parse_from_rfc3339("2025-01-03T04:05:06+00:00")
                            .expect("valid timestamp"),
                    ),
                ),
                ("latency".to_string(), RuntimeValue::F64(OrderedFloat(7.25))),
                ("active".to_string(), RuntimeValue::Bool(false)),
            ]),
        ];

        let batch = concat_test_rows(&records).expect("rows should concatenate as Arrow");
        assert_eq!(batch.batch().num_rows(), 2);
        assert_eq!(batch.value(0, "user_id"), Ok(Some(RuntimeValue::U32(42))));
        assert_eq!(
            batch.value(1, "tenant"),
            Ok(Some(RuntimeValue::String("beta".to_string())))
        );
    }

    #[test]
    fn runtime_row_is_a_shared_view_over_an_arrow_batch() {
        let batch = record().one_row_batch();
        let batch = Arc::new(batch);
        let row = RuntimeRow::new(batch.clone(), 0, RuntimeRecordMetadata::test())
            .expect("batch contains one row");

        assert!(Arc::ptr_eq(row.batch(), &batch));
        assert_eq!(
            row.value("tenant").expect("tenant should be readable"),
            Some(RuntimeValue::String("acme".to_string()))
        );
    }

    #[test]
    fn optional_fields_roundtrip_through_arrow_batch_as_nulls() {
        let compiled = compile_schema(&optional_schema());
        assert!(compiled.arrow_schema().field(1).is_nullable());

        let batch = compiled
            .batch_from_test_rows([[("user_id".to_string(), RuntimeValue::U32(42))]])
            .expect("values should build an Arrow batch");
        let nickname = batch
            .batch()
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("nickname column should be strings");
        assert!(nickname.is_null(0));

        assert_eq!(batch.value(0, "user_id"), Ok(Some(RuntimeValue::U32(42))));
        assert_eq!(batch.value(0, "nickname"), Ok(None));
    }

    #[test]
    fn runtime_arrow_batches_can_be_concatenated() {
        let left = record().one_row_batch();
        let right_record = test_runtime_row([
            ("user_id".to_string(), RuntimeValue::U32(7)),
            (
                "tenant".to_string(),
                RuntimeValue::String("beta".to_string()),
            ),
            (
                "created_at".to_string(),
                RuntimeValue::Datetime(
                    DateTime::parse_from_rfc3339("2025-01-03T04:05:06+00:00")
                        .expect("valid timestamp"),
                ),
            ),
            ("latency".to_string(), RuntimeValue::F64(OrderedFloat(7.25))),
            ("active".to_string(), RuntimeValue::Bool(false)),
        ]);
        let right = right_record.one_row_batch();

        let concatenated =
            RuntimeRecordBatch::concat(&[&left, &right]).expect("batches should concat");

        assert_eq!(concatenated.batch().num_rows(), 2);
        assert_eq!(
            concatenated.value(0, "user_id"),
            Ok(Some(RuntimeValue::U32(42)))
        );
        assert_eq!(
            concatenated.value(1, "user_id"),
            Ok(Some(RuntimeValue::U32(7)))
        );
    }

    #[rstest]
    #[case::json(NotificationCodecCase::Json)]
    #[case::avro(NotificationCodecCase::Avro)]
    #[case::schemaful_cbor(NotificationCodecCase::SchemafulCbor)]
    #[case::jaq_native_cbor(NotificationCodecCase::JaqNativeCbor)]
    fn codec_roundtrips_runtime_records(
        compiled_notification_schema: Arc<CompiledSchema>,
        notification_record: RuntimeRow,
        #[case] codec_case: NotificationCodecCase,
    ) {
        let compiled_codec = codec_case.compile(compiled_notification_schema.clone());
        let payload =
            encode_arrow_record(&compiled_codec, &notification_record).expect("must encode");
        let decoded = decode_one(&compiled_codec, &payload).expect("must decode");

        for field in compiled_notification_schema.fields() {
            assert_eq!(
                single_batch_value(&decoded, &field.name),
                row_value(&notification_record, &field.name),
                "field {} should roundtrip through {codec_case:?}",
                field.name
            );
        }
    }

    #[test]
    fn syslog_codec_decodes_rfc5424_directly_into_arrow() {
        let codec = compiled_syslog_codec();
        let payload = b"<34>1 2003-10-11T22:14:15.003Z edge-1 orders 123 ID47 \
                        [exampleSDID@32473 iut=\"3\" note=\"a\\]b\"] order accepted\r\n\0";
        let decoded = decode_one(&codec, payload).expect("RFC 5424 should decode");

        assert_eq!(
            single_batch_value(&decoded, "facility"),
            Some(RuntimeValue::U8(4))
        );
        assert_eq!(
            single_batch_value(&decoded, "severity"),
            Some(RuntimeValue::U8(2))
        );
        assert_eq!(
            single_batch_value(&decoded, "timestamp"),
            Some(RuntimeValue::Datetime(
                DateTime::parse_from_rfc3339("2003-10-11T22:14:15.003Z").expect("valid timestamp")
            ))
        );
        assert_eq!(
            single_batch_value(&decoded, "hostname"),
            Some(RuntimeValue::String("edge-1".to_string()))
        );
        assert_eq!(
            single_batch_value(&decoded, "structured_data"),
            Some(RuntimeValue::String(
                "[exampleSDID@32473 iut=\"3\" note=\"a\\]b\"]".to_string()
            ))
        );
        assert_eq!(
            single_batch_value(&decoded, "message"),
            Some(RuntimeValue::String("order accepted".to_string()))
        );
    }

    #[test]
    fn syslog_codec_decodes_rfc3164_and_defaults_missing_priority() {
        let codec = compiled_syslog_codec();
        let decoded = decode_one(
            &codec,
            b"<13>Feb  5 17:32:18 relay.example payments: settled",
        )
        .expect("RFC 3164 should decode");
        let timestamp = single_batch_value(&decoded, "timestamp")
            .expect("RFC 3164 timestamp should be present");
        let RuntimeValue::Datetime(timestamp) = timestamp else {
            panic!("timestamp must be a DATETIME")
        };
        assert_eq!(timestamp.year(), Utc::now().year());
        assert_eq!(timestamp.offset().local_minus_utc(), 0);
        assert_eq!(
            single_batch_value(&decoded, "hostname"),
            Some(RuntimeValue::String("relay.example".to_string()))
        );
        assert_eq!(
            single_batch_value(&decoded, "app_name"),
            Some(RuntimeValue::String("payments".to_string()))
        );

        let defaulted =
            decode_one(&codec, b"plain syslog message").expect("message without PRI should decode");
        assert_eq!(
            single_batch_value(&defaulted, "facility"),
            Some(RuntimeValue::U8(1))
        );
        assert_eq!(
            single_batch_value(&defaulted, "severity"),
            Some(RuntimeValue::U8(5))
        );
        assert_eq!(
            single_batch_value(&defaulted, "message"),
            Some(RuntimeValue::String("plain syslog message".to_string()))
        );
    }

    #[test]
    fn syslog_codec_handles_rfc_priority_timestamp_tag_and_bom_edges() {
        let codec = compiled_syslog_codec();
        let malformed_priority = decode_one(&codec, b"<013>plain syslog message")
            .expect("malformed PRI should use the relay default");
        assert_eq!(
            single_batch_value(&malformed_priority, "facility"),
            Some(RuntimeValue::U8(1))
        );
        assert_eq!(
            single_batch_value(&malformed_priority, "severity"),
            Some(RuntimeValue::U8(5))
        );
        assert_eq!(
            single_batch_value(&malformed_priority, "message"),
            Some(RuntimeValue::String(
                "<013>plain syslog message".to_string()
            ))
        );

        let tagged = decode_one(
            &codec,
            b"<13>Feb  5 17:32:18 relay.example worker[42]: restarted",
        )
        .expect("RFC 3164 TAG and process suffix should decode");
        assert_eq!(
            single_batch_value(&tagged, "app_name"),
            Some(RuntimeValue::String("worker".to_string()))
        );
        assert_eq!(
            single_batch_value(&tagged, "message"),
            Some(RuntimeValue::String("restarted".to_string()))
        );

        let bom = decode_one(
            &codec,
            b"<34>1 2003-10-11T22:14:15.003Z edge app 1 ID - \xef\xbb\xbfunicode",
        )
        .expect("RFC 5424 UTF-8 BOM should decode");
        assert_eq!(
            single_batch_value(&bom, "message"),
            Some(RuntimeValue::String("unicode".to_string()))
        );

        for timestamp in [
            "2003-08-24T05:14:15.000000003-07:00",
            "2003-10-11t22:14:15.003z",
        ] {
            let payload = format!("<34>1 {timestamp} edge app 1 ID - invalid timestamp");
            assert!(
                decode_one(&codec, payload.as_bytes()).is_err(),
                "RFC 5424 timestamp '{timestamp}' must be rejected"
            );
        }
    }

    #[test]
    fn syslog_codec_accepts_rfc5424_control_values_while_preserving_structured_data() {
        let codec = compiled_syslog_codec();
        let payload = b"<34>1 - edge app 1 ID [example@32473 note=\"line\tvalue\"] body";
        let decoded = decode_one(&codec, payload)
            .expect("control characters are valid in an RFC 5424 PARAM-VALUE");
        assert_eq!(
            single_batch_value(&decoded, "structured_data"),
            Some(RuntimeValue::String(
                "[example@32473 note=\"line\tvalue\"]".to_string()
            ))
        );
    }

    #[test]
    fn syslog_codec_encodes_rfc5424_with_nilvalues() {
        let schema_model = CreateSchema {
            name: named("syslog_minimal"),
            fields: syslog_schema()
                .fields
                .into_iter()
                .filter(|field| matches!(field.name.as_str(), "facility" | "severity" | "message"))
                .collect(),
        };
        let codec_model = CreateCodec {
            schema: schema_model.name.clone(),
            ..syslog_codec()
        };
        let codec = compile_codec(
            &codec_model,
            Arc::new(compile_schema(&schema_model)),
            self_describing(&codec_model.wire_format),
        )
        .expect("minimal encoding schema should compile");
        let payload = encode_test_fields(
            &codec,
            [
                ("facility".to_string(), RuntimeValue::U8(4)),
                ("severity".to_string(), RuntimeValue::U8(2)),
                (
                    "message".to_string(),
                    RuntimeValue::String("\u{feff}order accepted".to_string()),
                ),
            ],
        )
        .expect("SYSLOG record should encode");

        assert_eq!(payload, b"<34>1 - - - - - - order accepted".to_vec());
    }

    #[test]
    fn syslog_codec_caps_encoded_timestamp_precision_at_microseconds() {
        let codec = compiled_syslog_codec();
        let payload = encode_test_fields(
            &codec,
            [
                ("facility".to_string(), RuntimeValue::U8(4)),
                ("severity".to_string(), RuntimeValue::U8(2)),
                (
                    "timestamp".to_string(),
                    RuntimeValue::Datetime(
                        DateTime::parse_from_rfc3339("2003-10-11T22:14:15.123456789Z")
                            .expect("valid test timestamp"),
                    ),
                ),
                (
                    "message".to_string(),
                    RuntimeValue::String("precise".to_string()),
                ),
            ],
        )
        .expect("SYSLOG timestamp should encode");
        assert_eq!(
            payload,
            b"<34>1 2003-10-11T22:14:15.123456Z - - - - - precise"
        );
    }

    #[test]
    fn syslog_codec_rejects_invalid_record_headers_and_structured_data() {
        let codec = compiled_syslog_codec();
        let common = || {
            vec![
                ("facility".to_string(), RuntimeValue::U8(4)),
                ("severity".to_string(), RuntimeValue::U8(2)),
                (
                    "message".to_string(),
                    RuntimeValue::String("order accepted".to_string()),
                ),
            ]
        };
        let mut invalid_header = common();
        invalid_header.push((
            "hostname".to_string(),
            RuntimeValue::String("not valid".to_string()),
        ));
        let error =
            encode_test_fields(&codec, invalid_header).expect_err("header spaces must be rejected");
        assert!(matches!(error, CodecError::EncodeField { ref field, .. } if field == "hostname"));

        let mut invalid_sd = common();
        invalid_sd.push((
            "structured_data".to_string(),
            RuntimeValue::String("[example value=\"unterminated]".to_string()),
        ));
        let error = encode_test_fields(&codec, invalid_sd)
            .expect_err("malformed structured data must be rejected");
        assert!(
            matches!(error, CodecError::EncodeField { ref field, .. } if field == "structured_data")
        );

        let mut duplicate_sd = common();
        duplicate_sd.push((
            "structured_data".to_string(),
            RuntimeValue::String("[example value=\"one\"][example value=\"two\"]".to_string()),
        ));
        let error = encode_test_fields(&codec, duplicate_sd)
            .expect_err("duplicate structured-data IDs must be rejected");
        assert!(
            matches!(error, CodecError::EncodeField { ref field, .. } if field == "structured_data")
        );

        let mut duplicate_parameter = common();
        duplicate_parameter.push((
            "structured_data".to_string(),
            RuntimeValue::String("[example value=\"one\" value=\"two\"]".to_string()),
        ));
        let error = encode_test_fields(&codec, duplicate_parameter)
            .expect_err("duplicate structured-data parameter names must be rejected");
        assert!(
            matches!(error, CodecError::EncodeField { ref field, .. } if field == "structured_data")
        );

        let mut invalid_priority = common();
        invalid_priority[0] = ("facility".to_string(), RuntimeValue::U8(24));
        let error = encode_test_fields(&codec, invalid_priority)
            .expect_err("facility above 23 must be rejected");
        assert!(matches!(error, CodecError::EncodeField { ref field, .. } if field == "facility"));
    }

    #[test]
    fn json_codec_encodes_arrow_rows_as_a_batch() {
        let compiled_schema = Arc::new(compile_schema(&schema()));
        let compiled_codec = compile_codec(
            &codec("json_codec"),
            compiled_schema.clone(),
            ResolvedCodecWireFormat::Json(&json_wire_schema()),
        )
        .expect("codec should compile");
        let records = [record(), record_with(7, "acme")];
        let batch = concat_test_rows(&records).expect("rows should concatenate as Arrow");

        let encoder = compiled_codec
            .batch_encoder(&batch)
            .expect("arrow batch should be accepted");
        let mut payloads = Vec::with_capacity(records.len());
        for row_index in 0..records.len() {
            let mut payload = Vec::new();
            encoder
                .encode_row_into(row_index, &mut payload)
                .expect("arrow row should encode directly");
            payloads.push(payload);
        }

        assert_eq!(payloads.len(), records.len());
        for (payload, expected_user_id) in payloads.iter().zip([42, 7]) {
            let decoded =
                decode_one(&compiled_codec, payload).expect("columnar JSON payload should decode");
            assert_eq!(
                single_batch_value(&decoded, "user_id"),
                Some(RuntimeValue::U32(expected_user_id))
            );
        }

        let mut second_payload = Vec::new();
        encoder
            .encode_row_into(1, &mut second_payload)
            .expect("a selected row should encode");
        assert_eq!(second_payload, payloads[1]);
        let error = encoder
            .encode_row_into(2, &mut second_payload)
            .expect_err("an out-of-bounds row must fail");
        assert!(matches!(error, CodecError::InvalidCodec { .. }));
    }

    #[test]
    fn codec_batch_encoder_reuses_payload_storage_for_arrow_rows() {
        let compiled_schema = Arc::new(compile_schema(&schema()));
        let compiled_codec = compile_codec(
            &codec("json_codec"),
            compiled_schema.clone(),
            ResolvedCodecWireFormat::Json(&json_wire_schema()),
        )
        .expect("codec should compile");
        let rows = [record_with(42, &"a".repeat(4_096)), record_with(42, "b")];
        let batch = concat_test_rows(&rows).expect("rows should concatenate as Arrow");
        let encoder = compiled_codec
            .batch_encoder(&batch)
            .expect("arrow batch should be accepted");
        let mut payload = Vec::new();

        encoder
            .encode_row_into(0, &mut payload)
            .expect("first arrow row should encode");
        let allocation = payload.as_ptr();
        let capacity = payload.capacity();
        encoder
            .encode_row_into(1, &mut payload)
            .expect("second arrow row should encode into the same buffer");

        assert_eq!(payload.as_ptr(), allocation);
        assert_eq!(payload.capacity(), capacity);
        let decoded = decode_one(&compiled_codec, &payload)
            .expect("reused payload should contain only the second row");
        assert_eq!(
            single_batch_value(&decoded, "tenant"),
            Some(RuntimeValue::String("b".to_string()))
        );
    }

    #[test]
    fn codec_batch_encoder_does_not_eagerly_encode_arrow_rows() {
        let schema_model = CreateSchema {
            name: named("counter"),
            fields: vec![SchemaField {
                name: named("value"),
                ty: ParseAsType::U64,
                optional: false,
                sensitive: false,
            }],
        };
        let compiled_schema = Arc::new(compile_schema(&schema_model));
        let codec_model = CreateCodec {
            name: named("counter_avro"),
            wire_format: CodecWireFormat::Avro {
                wire_schema: named("counter_wire"),
            },
            schema: schema_model.name.clone(),
            encoding_rules: Vec::new(),
        };
        let wire_schema = CreateWireSchema {
            name: named("counter_wire"),
            strictness: Default::default(),
            fields: vec![WireSchemaField {
                name: named("value"),
                ty: AvroType::Long,
                optional: false,
            }],
        };
        let compiled_codec = compile_codec(
            &codec_model,
            compiled_schema.clone(),
            ResolvedCodecWireFormat::Avro(&wire_schema),
        )
        .expect("codec should compile");
        let batch = compiled_schema
            .batch_from_test_rows([
                [("value".to_string(), RuntimeValue::U64(1))],
                [("value".to_string(), RuntimeValue::U64(u64::MAX))],
            ])
            .expect("values should build an Arrow batch");

        let encoder = compiled_codec
            .batch_encoder(&batch)
            .expect("building an encoder must not serialize later rows");
        let mut payload = Vec::new();
        encoder
            .encode_row_into(0, &mut payload)
            .expect("the first row should encode independently");
        let error = encoder
            .encode_row_into(1, &mut payload)
            .expect_err("the overflowing second row should fail only when requested");

        assert!(matches!(error, CodecError::EncodeField { .. }));
    }

    #[test]
    fn avro_codec_decodes_wire_fields_into_internal_arrow_order() {
        let compiled_schema = Arc::new(compile_schema(&schema()));
        let mut wire_schema = avro_wire_schema();
        wire_schema.fields.rotate_left(2);
        let compiled_codec = compile_codec(
            &codec("avro_codec"),
            compiled_schema,
            ResolvedCodecWireFormat::Avro(&wire_schema),
        )
        .expect("codec should compile");

        let payload = encode_arrow_record(&compiled_codec, &record()).expect("must encode");
        let decoded = decode_one(&compiled_codec, &payload).expect("must decode");

        assert_eq!(decoded.batch().schema().field(0).name(), "user_id");
        assert_eq!(decoded.batch().schema().field(1).name(), "tenant");
        assert_eq!(
            single_batch_value(&decoded, "user_id"),
            Some(RuntimeValue::U32(42))
        );
        assert_eq!(
            single_batch_value(&decoded, "tenant"),
            Some(RuntimeValue::String("acme".to_string()))
        );
    }

    #[test]
    fn json_codec_and_arrow_support_array_and_vector_fields() {
        let compiled_schema = Arc::new(compile_schema(&array_schema()));
        let compiled_codec = compile_codec(
            &array_codec("json_array_codec"),
            compiled_schema.clone(),
            ResolvedCodecWireFormat::Json(&array_json_wire_schema()),
        )
        .expect("codec should compile");

        let decoded = decode_one(
            &compiled_codec,
            br#"{"cpu_last_64":[1.0,2.5,3.25],"labels":["prod","api"]}"#,
        )
        .expect("array payload should decode");
        assert_eq!(
            single_batch_value(&decoded, "cpu_last_64"),
            row_value(&array_record(), "cpu_last_64")
        );
        assert_eq!(
            single_batch_value(&decoded, "labels"),
            row_value(&array_record(), "labels")
        );

        let batch = decoded;
        assert!(matches!(
            batch.batch().schema().field(0).data_type(),
            ArrowDataType::FixedSizeList(_, 3)
        ));
        assert!(matches!(
            batch.batch().schema().field(1).data_type(),
            ArrowDataType::List(_)
        ));

        assert_eq!(
            single_batch_value(&batch, "cpu_last_64"),
            row_value(&array_record(), "cpu_last_64")
        );
        assert_eq!(
            single_batch_value(&batch, "labels"),
            row_value(&array_record(), "labels")
        );
    }

    /// The rows of a batch are decoded into one builder, so a payload the codec rejects must not
    /// disturb the rows around it. The failure here lands after a column has already been written,
    /// which is the case that leaves a half-written row behind.
    #[test]
    fn batch_builder_drops_the_row_a_failed_decode_started() {
        let schema = Arc::new(compile_schema(&array_schema()));
        let codec = compile_codec(
            &array_codec("json_array_codec"),
            schema.clone(),
            ResolvedCodecWireFormat::Json(&array_json_wire_schema()),
        )
        .expect("array codec fixture should compile");

        let mut builder = schema.batch_builder(3);
        decode_with_codec(
            &codec,
            Cow::Borrowed(br#"{"cpu_last_64":[1.0,2.5,3.25],"labels":["prod"]}"#),
            &mut builder,
        )
        .expect("the first payload should decode");
        let rejected = decode_with_codec(
            &codec,
            Cow::Borrowed(br#"{"cpu_last_64":[1.0,"two",3.25],"labels":["api"]}"#),
            &mut builder,
        )
        .expect_err("an array element of the wrong type should be rejected");
        assert!(
            rejected.to_string().contains("cpu_last_64"),
            "the error should name the field that failed, got {rejected}"
        );
        decode_with_codec(
            &codec,
            Cow::Borrowed(br#"{"cpu_last_64":[4.0,5.0,6.0],"labels":["batch"]}"#),
            &mut builder,
        )
        .expect("the payload after the rejected one should decode");

        assert_eq!(builder.rows(), 2);
        let batch = builder.finish().expect("the batch should build");
        assert_eq!(batch.batch().num_rows(), 2);
        assert_eq!(
            batch.value(0, "cpu_last_64").expect("readable"),
            Some(RuntimeValue::Array(vec![
                RuntimeValue::F32(OrderedFloat(1.0)),
                RuntimeValue::F32(OrderedFloat(2.5)),
                RuntimeValue::F32(OrderedFloat(3.25)),
            ]))
        );
        assert_eq!(
            batch.value(0, "labels").expect("readable"),
            Some(RuntimeValue::Vec(vec![RuntimeValue::String(
                "prod".to_string()
            )]))
        );
        assert_eq!(
            batch.value(1, "cpu_last_64").expect("readable"),
            Some(RuntimeValue::Array(vec![
                RuntimeValue::F32(OrderedFloat(4.0)),
                RuntimeValue::F32(OrderedFloat(5.0)),
                RuntimeValue::F32(OrderedFloat(6.0)),
            ]))
        );
        assert_eq!(
            batch.value(1, "labels").expect("readable"),
            Some(RuntimeValue::Vec(vec![RuntimeValue::String(
                "batch".to_string()
            )]))
        );
    }

    /// A nested list leaves a half-written value inside a child builder, which the columns around
    /// it cannot see. The rows that did decode must still come out whole.
    #[test]
    fn batch_builder_drops_a_row_a_failed_nested_array_element_started() {
        let schema = Arc::new(compile_schema(&multidimensional_array_schema()));
        let codec = compile_codec(
            &CreateCodec {
                name: named("shaped_metrics_json_codec"),
                wire_format: CodecWireFormat::Json {
                    wire_schema: named("shaped_metrics_json"),
                },
                schema: named("shaped_metrics"),
                encoding_rules: Vec::new(),
            },
            schema.clone(),
            ResolvedCodecWireFormat::Json(&CreateWireSchema {
                name: named("shaped_metrics_json"),
                strictness: Default::default(),
                fields: ["matrix", "samples"]
                    .into_iter()
                    .map(|name| WireSchemaField {
                        name: named(name),
                        ty: JsonType::Array,
                        optional: false,
                    })
                    .collect(),
            }),
        )
        .expect("multidimensional JSON codec should compile");

        let mut builder = schema.batch_builder(2);
        for payload in [
            br#"{"matrix":[[1.0,2.0,3.0],[4.0,"five",6.0]],"samples":[[1.0,2.0]]}"#.as_slice(),
            br#"{"matrix":[[1.0,2.0,3.0],[4.0,5.0,6.0]],"samples":[[7.0,"eight"]]}"#.as_slice(),
        ] {
            decode_with_codec(&codec, Cow::Borrowed(payload), &mut builder)
                .expect_err("an element of the wrong type should be rejected");
        }
        let expected = multidimensional_array_record();
        let payload = encode_arrow_record(&codec, &expected).expect("must encode nested arrays");
        decode_with_codec(&codec, Cow::Borrowed(&payload), &mut builder)
            .expect("the payload after the rejected ones should decode");

        assert_eq!(builder.rows(), 1);
        let batch = builder.finish().expect("the batch should build");
        assert_eq!(batch.batch().num_rows(), 1);
        for field in schema.fields() {
            assert_eq!(
                single_batch_value(&batch, &field.name),
                row_value(&expected, &field.name)
            );
        }
    }

    /// A caller that decodes rows it then cannot accept takes them back out of the batch.
    #[test]
    fn batch_builder_drops_the_rows_a_caller_could_not_accept() {
        let schema = Arc::new(compile_schema(&array_schema()));
        let codec = compile_codec(
            &array_codec("json_array_codec"),
            schema.clone(),
            ResolvedCodecWireFormat::Json(&array_json_wire_schema()),
        )
        .expect("array codec fixture should compile");

        let mut builder = schema.batch_builder(3);
        for label in ["prod", "api", "batch"] {
            decode_with_codec(
                &codec,
                Cow::Owned(
                    format!(r#"{{"cpu_last_64":[1.0,2.5,3.25],"labels":["{label}"]}}"#)
                        .into_bytes(),
                ),
                &mut builder,
            )
            .expect("every payload should decode");
        }

        builder.abandon_rows_after(1);
        assert_eq!(builder.rows(), 1);
        let batch = builder.finish().expect("the batch should build");
        assert_eq!(batch.batch().num_rows(), 1);
        assert_eq!(
            batch.value(0, "labels").expect("readable"),
            Some(RuntimeValue::Vec(vec![RuntimeValue::String(
                "prod".to_string()
            )]))
        );
    }

    #[test]
    fn arrow_roundtrips_multidimensional_fixed_and_variable_array_shapes() {
        let schema = compile_schema(&multidimensional_array_schema());
        let expected = multidimensional_array_record();
        let batch = expected.one_row_batch();

        let arrow_schema = batch.batch().schema();
        let ArrowDataType::FixedSizeList(matrix_rows, 2) = arrow_schema.field(0).data_type() else {
            panic!("matrix should use nested fixed-size lists");
        };
        assert!(matches!(
            matrix_rows.data_type(),
            ArrowDataType::FixedSizeList(_, 3)
        ));
        let ArrowDataType::List(samples) = arrow_schema.field(1).data_type() else {
            panic!("samples should use a variable outer list");
        };
        assert!(matches!(
            samples.data_type(),
            ArrowDataType::FixedSizeList(_, 2)
        ));

        for field in schema.fields() {
            assert_eq!(
                single_batch_value(&batch, &field.name),
                row_value(&expected, &field.name)
            );
        }
    }

    #[test]
    fn avro_codec_roundtrips_multidimensional_fixed_and_variable_array_shapes() {
        let schema = Arc::new(compile_schema(&multidimensional_array_schema()));
        let codec = CreateCodec {
            name: named("shaped_metrics_codec"),
            wire_format: CodecWireFormat::Avro {
                wire_schema: named("shaped_metrics_wire"),
            },
            schema: named("shaped_metrics"),
            encoding_rules: Vec::new(),
        };
        let codec = compile_codec(
            &codec,
            schema,
            ResolvedCodecWireFormat::Avro(&multidimensional_avro_wire_schema()),
        )
        .expect("multidimensional Avro codec should compile");
        let expected = multidimensional_array_record();

        let payload = encode_arrow_record(&codec, &expected).expect("must encode nested arrays");
        let decoded = decode_one(&codec, &payload).expect("must decode nested arrays");

        for field in codec.schema.fields() {
            assert_eq!(
                single_batch_value(&decoded, &field.name),
                row_value(&expected, &field.name)
            );
        }
    }

    #[rstest]
    #[case::avro(ArrayCodecCase::Avro)]
    #[case::cbor(ArrayCodecCase::Cbor)]
    #[case::yaml(ArrayCodecCase::Yaml)]
    fn codec_roundtrips_array_and_vector_fields(
        compiled_array_schema: Arc<CompiledSchema>,
        array_record_fixture: RuntimeRow,
        #[case] codec_case: ArrayCodecCase,
    ) {
        let compiled_codec = codec_case.compile(compiled_array_schema);
        let payload =
            encode_arrow_record(&compiled_codec, &array_record_fixture).expect("must encode");
        let decoded = decode_one(&compiled_codec, &payload).expect("must decode");

        assert_eq!(
            single_batch_value(&decoded, "cpu_last_64"),
            row_value(&array_record_fixture, "cpu_last_64")
        );
        assert_eq!(
            single_batch_value(&decoded, "labels"),
            row_value(&array_record_fixture, "labels")
        );
    }

    #[test]
    fn json_codec_and_arrow_support_arrays_and_vectors_for_all_primitive_types() {
        let expected = primitive_arrays_record();
        let compiled_schema = Arc::new(compile_schema(&primitive_arrays_schema()));
        let compiled_codec = compile_codec(
            &primitive_arrays_codec("json_primitive_arrays_codec"),
            compiled_schema.clone(),
            ResolvedCodecWireFormat::Json(&primitive_arrays_json_wire_schema()),
        )
        .expect("codec should compile");

        let decoded = decode_one(&compiled_codec, &primitive_arrays_json_payload())
            .expect("primitive array payload should decode");
        for field in compiled_schema.fields() {
            assert_eq!(
                single_batch_value(&decoded, &field.name),
                row_value(&expected, &field.name),
                "field {} should decode",
                field.name
            );
        }

        let batch = decoded;
        for field in compiled_schema.fields() {
            assert_eq!(
                single_batch_value(&batch, &field.name),
                row_value(&expected, &field.name),
                "field {} should roundtrip through Arrow",
                field.name
            );
        }
    }

    #[rstest]
    #[case::avro(PrimitiveArrayCodecCase::Avro)]
    #[case::cbor(PrimitiveArrayCodecCase::Cbor)]
    #[case::toml(PrimitiveArrayCodecCase::Toml)]
    fn codec_roundtrips_arrays_and_vectors_for_all_primitive_types(
        compiled_primitive_array_schema: Arc<CompiledSchema>,
        primitive_array_record_fixture: RuntimeRow,
        #[case] codec_case: PrimitiveArrayCodecCase,
    ) {
        let compiled_codec = codec_case.compile(compiled_primitive_array_schema.clone());
        let payload = encode_arrow_record(&compiled_codec, &primitive_array_record_fixture)
            .expect("must encode");
        let decoded = decode_one(&compiled_codec, &payload).expect("must decode");

        for field in compiled_primitive_array_schema.fields() {
            assert_eq!(
                single_batch_value(&decoded, &field.name),
                row_value(&primitive_array_record_fixture, &field.name),
                "field {} should roundtrip through {codec_case:?}",
                field.name
            );
        }
    }

    #[test]
    fn json_decode_rejects_missing_or_incompatible_fields() {
        let compiled_schema = Arc::new(compile_schema(&schema()));
        let compiled_codec = compile_codec(
            &codec("json_codec"),
            compiled_schema,
            ResolvedCodecWireFormat::Json(&json_wire_schema()),
        )
        .expect("codec should compile");

        let missing = br#"{"user_id":42,"tenant":"acme","created_at":"2025-01-02T03:04:05+00:00","active":true}"#;
        let err = decode_one(&compiled_codec, missing).expect_err("must reject missing field");
        assert!(matches!(err, CodecError::MissingField { field, .. } if field == "latency"));

        let bad_type = br#"{"user_id":"forty-two","tenant":"acme","created_at":"2025-01-02T03:04:05+00:00","latency":12.5,"active":true}"#;
        let err = decode_one(&compiled_codec, bad_type).expect_err("must reject bad type");
        assert!(matches!(err, CodecError::ParseField { field, .. } if field == "user_id"));
    }

    #[test]
    fn strict_json_wire_schema_rejects_unknown_fields() {
        let compiled_schema = Arc::new(compile_schema(&schema()));
        let compiled_codec = compile_codec(
            &codec("json_codec"),
            compiled_schema,
            ResolvedCodecWireFormat::Json(&json_wire_schema_with_strictness(
                WireSchemaStrictness::Strict,
            )),
        )
        .expect("codec should compile");

        let err = decode_one(&compiled_codec, notification_json_payload_with_extra())
            .expect_err("strict wire schema should reject unknown fields");
        assert!(matches!(err, CodecError::UnexpectedField { field, .. } if field == "ignored"));
    }

    #[test]
    fn loose_json_wire_schema_drops_unknown_fields() {
        let compiled_schema = Arc::new(compile_schema(&schema()));
        let compiled_codec = compile_codec(
            &codec("json_codec"),
            compiled_schema,
            ResolvedCodecWireFormat::Json(&json_wire_schema_with_strictness(
                WireSchemaStrictness::Loose,
            )),
        )
        .expect("codec should compile");

        let decoded = decode_one(&compiled_codec, notification_json_payload_with_extra())
            .expect("loose wire schema should accept unknown fields");
        assert_eq!(single_batch_value(&decoded, "ignored"), None);
        assert_eq!(
            single_batch_value(&decoded, "user_id"),
            Some(RuntimeValue::U32(42))
        );
    }

    #[test]
    fn loose_cbor_wire_schema_drops_unknown_fields() {
        let compiled_schema = Arc::new(compile_schema(&schema()));
        let compiled_codec = compile_codec(
            &schemaful_cbor_codec("schemaful_cbor_codec"),
            compiled_schema,
            ResolvedCodecWireFormat::Cbor(&cbor_wire_schema(WireSchemaStrictness::Loose)),
        )
        .expect("codec should compile");

        let decoded = decode_one(&compiled_codec, &notification_cbor_payload_with_extra())
            .expect("loose cbor wire schema should accept unknown fields");
        assert_eq!(single_batch_value(&decoded, "ignored"), None);
        assert_eq!(
            single_batch_value(&decoded, "user_id"),
            Some(RuntimeValue::U32(42))
        );
    }

    #[test]
    fn json_codec_accepts_missing_and_null_optional_fields() {
        let compiled_schema = Arc::new(compile_schema(&optional_schema()));
        let compiled_codec = compile_codec(
            &optional_codec("json_optional_codec"),
            compiled_schema,
            ResolvedCodecWireFormat::Json(&optional_json_wire_schema()),
        )
        .expect("codec should compile");

        let missing = decode_one(&compiled_codec, br#"{"user_id":42}"#)
            .expect("missing optional field should decode");
        assert_eq!(
            single_batch_value(&missing, "user_id"),
            Some(RuntimeValue::U32(42))
        );
        assert_eq!(single_batch_value(&missing, "nickname"), None);

        let explicit_null = decode_one(&compiled_codec, br#"{"user_id":7,"nickname":null}"#)
            .expect("null optional field should decode");
        assert_eq!(
            single_batch_value(&explicit_null, "user_id"),
            Some(RuntimeValue::U32(7))
        );
        assert_eq!(single_batch_value(&explicit_null, "nickname"), None);
    }

    #[test]
    fn json_codec_omits_missing_optional_fields_on_encode() {
        let compiled_schema = Arc::new(compile_schema(&optional_schema()));
        let compiled_codec = compile_codec(
            &optional_codec("json_optional_codec"),
            compiled_schema,
            ResolvedCodecWireFormat::Json(&optional_json_wire_schema()),
        )
        .expect("codec should compile");

        let payload = encode_test_fields(
            &compiled_codec,
            [("user_id".to_string(), RuntimeValue::U32(42))],
        )
        .expect("must encode");
        assert_eq!(
            String::from_utf8(payload).expect("valid json"),
            r#"{"user_id":42}"#
        );
    }

    #[test]
    fn avro_codec_roundtrips_missing_optional_fields_as_null() {
        let compiled_schema = Arc::new(compile_schema(&optional_schema()));
        let compiled_codec = compile_codec(
            &optional_codec("avro_optional_codec"),
            compiled_schema,
            ResolvedCodecWireFormat::Avro(&optional_avro_wire_schema()),
        )
        .expect("codec should compile");

        let payload = encode_test_fields(
            &compiled_codec,
            [("user_id".to_string(), RuntimeValue::U32(42))],
        )
        .expect("must encode");
        let decoded = decode_one(&compiled_codec, &payload).expect("must decode");

        assert_eq!(
            single_batch_value(&decoded, "user_id"),
            Some(RuntimeValue::U32(42))
        );
        assert_eq!(single_batch_value(&decoded, "nickname"), None);
    }

    #[test]
    fn arrow_batch_rejects_incompatible_runtime_values_before_encoding() {
        let compiled_schema = Arc::new(compile_schema(&schema()));
        let err = compiled_schema
            .batch_from_test_rows([[
                ("user_id".to_string(), RuntimeValue::U32(42)),
                (
                    "tenant".to_string(),
                    RuntimeValue::String("acme".to_string()),
                ),
                (
                    "created_at".to_string(),
                    RuntimeValue::Datetime(
                        DateTime::parse_from_rfc3339("2025-01-02T03:04:05+00:00")
                            .expect("valid timestamp"),
                    ),
                ),
                (
                    "latency".to_string(),
                    RuntimeValue::String("slow".to_string()),
                ),
                ("active".to_string(), RuntimeValue::Bool(true)),
            ]])
            .expect_err("must reject");
        assert!(err.contains("latency"));
    }

    #[test]
    fn persisted_runtime_record_restores_directly_into_an_arrow_row() {
        let record = record().with_ingested_at_watermarks(Timestamp::from_unix_nanos(1_234_567));
        assert_eq!(
            record.to_json_string().expect("Arrow row should serialize"),
            r#"{"active":true,"created_at":"2025-01-02T03:04:05+00:00","latency":12.5,"tenant":"acme","user_id":42}"#
        );

        let remote = record.to_remote().expect("Arrow row should persist");
        let roundtrip = compile_schema(&schema())
            .runtime_row_from_remote(remote)
            .expect("persisted row should restore into Arrow");
        assert_eq!(
            row_value(&roundtrip, "tenant"),
            Some(RuntimeValue::String("acme".to_string()))
        );
        assert_eq!(
            row_value(&roundtrip, "user_id"),
            Some(RuntimeValue::U32(42))
        );
        assert_eq!(
            roundtrip.metadata().ingested_at_low_watermark(),
            Timestamp::from_unix_nanos(1_234_567)
        );
        assert_eq!(
            roundtrip.metadata().ingested_at_high_watermark(),
            Timestamp::from_unix_nanos(1_234_567)
        );
    }

    #[test]
    fn runtime_value_serde_roundtrips_and_rejects_invalid_rfc3339() {
        let value = RuntimeValue::Datetime(
            DateTime::parse_from_rfc3339("2025-01-02T03:04:05+00:00").expect("valid timestamp"),
        );

        let json = serde_json::to_string(&value).expect("runtime value should serialize");
        let roundtrip: RuntimeValue =
            serde_json::from_str(&json).expect("runtime value should deserialize");
        assert_eq!(roundtrip, value);

        let err = serde_json::from_str::<RuntimeValue>(
            r#"{"type":"Datetime","value":"not-a-timestamp"}"#,
        )
        .expect_err("invalid timestamp must fail");
        assert!(!err.to_string().is_empty());
    }

    #[test]
    fn json_codec_rejects_non_object_payloads_and_missing_wire_fields() {
        let compiled_schema = Arc::new(compile_schema(&schema()));
        let compiled_codec = compile_codec(
            &codec("json_codec"),
            compiled_schema.clone(),
            ResolvedCodecWireFormat::Json(&json_wire_schema()),
        )
        .expect("codec should compile");

        let err = decode_one(&compiled_codec, br#"[1,2,3]"#).expect_err("arrays must be rejected");
        assert!(matches!(err, CodecError::ExpectedObject { .. }));

        let missing_wire_schema = CreateWireSchema {
            name: named("notification_wire_partial"),
            strictness: WireSchemaStrictness::Loose,
            fields: vec![
                WireSchemaField {
                    name: named("user_id"),
                    ty: JsonType::Integer,
                    optional: false,
                },
                WireSchemaField {
                    name: named("tenant"),
                    ty: JsonType::String,
                    optional: false,
                },
            ],
        };
        let missing_wire_codec = compile_codec(
            &CreateCodec {
                name: named("json_partial"),
                wire_format: CodecWireFormat::Json {
                    wire_schema: named("notification_wire_partial"),
                },
                schema: named("notification"),
                encoding_rules: Vec::new(),
            },
            compiled_schema,
            ResolvedCodecWireFormat::Json(&missing_wire_schema),
        )
        .expect("codec should compile");

        let err = decode_one(
            &missing_wire_codec,
            br#"{"user_id":42,"tenant":"acme","created_at":"2025-01-02T03:04:05+00:00","latency":12.5,"active":true}"#,
        )
        .expect_err("missing wire field must fail");
        assert!(
            matches!(err, CodecError::InvalidCodec { reason, .. } if reason.contains("created_at"))
        );
    }

    #[test]
    fn arrow_batch_rejects_missing_required_runtime_fields_before_encoding() {
        let compiled_schema = Arc::new(compile_schema(&schema()));
        let error = compiled_schema
            .batch_from_test_rows([[
                ("user_id".to_string(), RuntimeValue::U32(42)),
                (
                    "tenant".to_string(),
                    RuntimeValue::String("acme".to_string()),
                ),
            ]])
            .expect_err("missing required field must fail before encoding");
        assert!(error.contains("created_at"));
    }

    #[test]
    fn jaq_native_json_codec_applies_transformation_on_ingestion_before_decoding() {
        let compiled_schema = Arc::new(compile_schema(&schema()));
        let codec = jaq_native_codec(
            "json_with_jaq",
            CodecJaqFormat::Json,
            "notification",
            Some(".payload"),
            None,
        );
        let wire_format = self_describing(&codec.wire_format);
        let compiled_codec =
            compile_codec(&codec, compiled_schema, wire_format).expect("codec should compile");

        let decoded = decode_one(
            &compiled_codec,
            br#"{"payload":{"user_id":42,"tenant":"acme","created_at":"2025-01-02T03:04:05+00:00","latency":12.5,"active":true}}"#,
        )
        .expect("must decode");

        assert_eq!(
            single_batch_value(&decoded, "user_id"),
            Some(RuntimeValue::U32(42))
        );
        assert_eq!(
            single_batch_value(&decoded, "tenant"),
            Some(RuntimeValue::String("acme".to_string()))
        );
    }

    #[test]
    fn jaq_native_json_codec_applies_transformation_on_emitting_before_encoding() {
        let compiled_schema = Arc::new(compile_schema(&schema()));
        let codec = jaq_native_codec(
            "json_with_emitting_jaq",
            CodecJaqFormat::Json,
            "notification",
            None,
            Some("{payload: .}"),
        );
        let wire_format = self_describing(&codec.wire_format);
        let compiled_codec =
            compile_codec(&codec, compiled_schema, wire_format).expect("codec should compile");

        let payload = encode_arrow_record(&compiled_codec, &record()).expect("must encode");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&payload).expect("valid json"),
            serde_json::json!({
                "payload": {
                    "active": true,
                    "created_at": "2025-01-02T03:04:05+00:00",
                    "latency": 12.5,
                    "tenant": "acme",
                    "user_id": 42
                }
            })
        );
    }

    #[test]
    fn jaq_native_codec_rejects_invalid_ingestion_jaq_program() {
        let compiled_schema = Arc::new(compile_schema(&schema()));
        let codec = jaq_native_codec(
            "json_with_bad_ingestion_jaq",
            CodecJaqFormat::Json,
            "notification",
            Some(". | "),
            None,
        );
        let wire_format = self_describing(&codec.wire_format);
        let err =
            compile_codec(&codec, compiled_schema, wire_format).expect_err("invalid jaq must fail");

        assert!(matches!(err, CodecError::InvalidJaqTransformation { .. }));
    }

    #[test]
    fn jaq_native_codec_rejects_invalid_emitting_jaq_program() {
        let compiled_schema = Arc::new(compile_schema(&schema()));
        let codec = jaq_native_codec(
            "json_with_bad_emitting_jaq",
            CodecJaqFormat::Json,
            "notification",
            None,
            Some(". | "),
        );
        let wire_format = self_describing(&codec.wire_format);
        let err =
            compile_codec(&codec, compiled_schema, wire_format).expect_err("invalid jaq must fail");

        assert!(matches!(err, CodecError::InvalidJaqTransformation { .. }));
    }

    #[test]
    fn protobuf_codec_applies_transformation_on_ingestion_before_decoding() {
        let codec = protobuf_codec("protobuf_ingest", Some("."), None);
        let compiled_schema = Arc::new(compile_schema(&protobuf_schema()));
        let compiled_codec = compile_codec_with_protobuf(
            &codec,
            compiled_schema,
            self_describing(&codec.wire_format),
            Some(protobuf_descriptor()),
        )
        .expect("codec should compile");
        assert!(compiled_codec.requires_blocking_decode());
        assert!(!compiled_codec.requires_blocking_encode());

        let payload = [
            0x08, 42, 0x12, 4, b'a', b'c', b'm', b'e', 0x1a, 5, b'h', b'e', b'l', b'l', b'o',
        ];
        let decoded = decode_one(&compiled_codec, &payload).expect("must decode");

        assert_eq!(
            single_batch_value(&decoded, "user_id"),
            Some(RuntimeValue::U32(42))
        );
        assert_eq!(
            single_batch_value(&decoded, "tenant"),
            Some(RuntimeValue::String("acme".to_string()))
        );
        assert_eq!(
            single_batch_value(&decoded, "payload"),
            Some(RuntimeValue::String("hello".to_string()))
        );
    }

    #[test]
    fn protobuf_codec_applies_transformation_on_emitting_before_encoding() {
        let codec = protobuf_codec("protobuf_emit", None, Some("."));
        let compiled_schema = Arc::new(compile_schema(&protobuf_schema()));
        let compiled_codec = compile_codec_with_protobuf(
            &codec,
            compiled_schema,
            self_describing(&codec.wire_format),
            Some(protobuf_descriptor()),
        )
        .expect("codec should compile");
        assert!(!compiled_codec.requires_blocking_decode());
        assert!(compiled_codec.requires_blocking_encode());

        let record = test_runtime_row([
            ("user_id".to_string(), RuntimeValue::U32(42)),
            (
                "tenant".to_string(),
                RuntimeValue::String("acme".to_string()),
            ),
            (
                "payload".to_string(),
                RuntimeValue::String("hello".to_string()),
            ),
        ]);
        let payload = encode_arrow_record(&compiled_codec, &record).expect("must encode");

        assert_eq!(
            payload,
            vec![
                0x08, 42, 0x12, 4, b'a', b'c', b'm', b'e', 0x1a, 5, b'h', b'e', b'l', b'l', b'o',
            ]
        );
    }

    #[test]
    fn protobuf_codec_requires_compiled_descriptor() {
        let codec = protobuf_codec("protobuf_missing_descriptor", Some("."), None);
        let compiled_schema = Arc::new(compile_schema(&protobuf_schema()));
        let err = compile_codec_with_protobuf(
            &codec,
            compiled_schema,
            self_describing(&codec.wire_format),
            None,
        )
        .expect_err("descriptor is mandatory");

        assert!(
            matches!(err, CodecError::InvalidCodec { reason, .. } if reason.contains("compiled descriptor"))
        );
    }

    #[test]
    fn xml_codec_emits_runtime_records() {
        let compiled_schema = Arc::new(compile_schema(&schema()));
        let codec = jaq_native_codec(
            "xml_codec",
            CodecJaqFormat::Xml,
            "notification",
            None,
            Some(
                r#"{t: "notification", c: [{t: "user_id", c: [(.user_id | tostring)]}, {t: "tenant", c: [.tenant]}]}"#,
            ),
        );
        let wire_format = self_describing(&codec.wire_format);
        let compiled_codec =
            compile_codec(&codec, compiled_schema, wire_format).expect("codec should compile");
        let payload = encode_arrow_record(&compiled_codec, &record()).expect("must encode");

        assert_eq!(
            String::from_utf8(payload).expect("xml must be utf8"),
            "<notification><user_id>42</user_id><tenant>acme</tenant></notification>"
        );
    }
}
