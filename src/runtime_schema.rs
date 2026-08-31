use std::{io::Cursor, sync::Arc as StdArc};

use ahash::{HashMap, HashSet};
use apache_avro::{
    Schema as AvroSchema, from_avro_datum, to_avro_datum, types::Value as AvroValue,
};
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
};
use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
use arrow_schema::{
    DataType as ArrowDataType, Field as ArrowField, FieldRef as ArrowFieldRef,
    Schema as ArrowSchema, TimeUnit as ArrowTimeUnit,
};
use arrow_select::{
    concat::concat as concat_arrow_arrays, filter::filter_record_batch, take::take,
};
use chrono::{DateTime, FixedOffset};
use nervix_models::{
    AvroType, CodecJaqTransformations, CodecWireFormat, CreateCodec, CreateSchema,
    CreateWireSchema, Identifier, JsonType, ParseAsType, RemoteRuntimeElementValue,
    RemoteRuntimeField, RemoteRuntimeRecord, RemoteRuntimeRecordMetadata, RemoteRuntimeValue,
    Timestamp, WireSchemaDefinition, WireSchemaField, WireSchemaStrictness,
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
    pub name: Identifier,
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

#[derive(Debug, Clone)]
pub struct RuntimeRecordBatch {
    schema: StdArc<ArrowSchema>,
    batch: RecordBatch,
}

pub(crate) struct RuntimeRecordBatchBuilder {
    schema: StdArc<ArrowSchema>,
    fields: Vec<CompiledSchemaField>,
    builders: Vec<Box<dyn ArrayBuilder>>,
    rows: usize,
    next_column: usize,
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

    pub(crate) fn batch_builder(&self, capacity: usize) -> RuntimeRecordBatchBuilder {
        RuntimeRecordBatchBuilder {
            schema: self.arrow_schema.clone(),
            fields: self.fields.clone(),
            builders: self
                .fields
                .iter()
                .map(|field| make_builder(&arrow_data_type(&field.ty), capacity))
                .collect(),
            rows: 0,
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
        builder.finish()?.runtime_row(0, metadata)
    }

    fn validate_arrow_batch(&self, batch: &RuntimeRecordBatch) -> Result<(), String> {
        if batch.schema.as_ref() != self.arrow_schema.as_ref() {
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

    pub fn arrow_batch_from_ipc_bytes(&self, bytes: &[u8]) -> Result<RuntimeRecordBatch, String> {
        let mut reader =
            StreamReader::try_new(Cursor::new(bytes), None).map_err(|error| error.to_string())?;
        if reader.schema().as_ref() != self.arrow_schema.as_ref() {
            return Err("arrow ipc schema does not match compiled schema".to_string());
        }
        let batch = match reader.next() {
            Some(Ok(batch)) => batch,
            Some(Err(error)) => return Err(error.to_string()),
            None => return Err("arrow ipc payload contained no record batch".to_string()),
        };
        if let Some(next) = reader.next() {
            return match next {
                Ok(_) => Err("arrow ipc payload contained more than one record batch".to_string()),
                Err(error) => Err(error.to_string()),
            };
        }
        Ok(RuntimeRecordBatch {
            schema: self.arrow_schema.clone(),
            batch,
        })
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
        .expect("test Arrow row fields must have inferable types");
    let schema = StdArc::new(ArrowSchema::new(arrow_fields));
    let columns = fields
        .iter()
        .zip(schema.fields())
        .map(|((_, value), field)| runtime_value_arrow_array(field.data_type(), Some(value), 1))
        .collect::<Result<Vec<_>, _>>()
        .expect("test Arrow row values must match their inferred types");
    let batch = if columns.is_empty() {
        RecordBatch::try_new_with_options(
            schema.clone(),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(1)),
        )
    } else {
        RecordBatch::try_new(schema.clone(), columns)
    }
    .expect("test Arrow row batch must be valid");
    RuntimeRecordBatch::from_record_batch(schema, batch)
        .and_then(|batch| batch.runtime_row(0, RuntimeRecordMetadata::test()))
        .expect("test Arrow row view must be valid")
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

impl RuntimeValue {
    pub(crate) fn estimated_bytes(&self) -> u64 {
        match self {
            Self::U8(_) | Self::I8(_) | Self::Bool(_) => 1,
            Self::U16(_) | Self::I16(_) => 2,
            Self::U32(_) | Self::I32(_) | Self::F32(_) => 4,
            Self::U64(_) | Self::I64(_) | Self::F64(_) | Self::Datetime(_) => 8,
            Self::String(value) => u64::try_from(value.len()).unwrap_or(u64::MAX),
            Self::Array(values) | Self::Vec(values) => values
                .iter()
                .map(Self::estimated_bytes)
                .fold(0_u64, u64::saturating_add),
        }
    }
}

impl RuntimeRecordBatch {
    pub(crate) fn from_record_batch(
        expected_schema: StdArc<ArrowSchema>,
        batch: RecordBatch,
    ) -> Result<Self, String> {
        if batch.schema().as_ref() != expected_schema.as_ref() {
            return Err("arrow batch schema does not match expected schema".to_string());
        }
        Ok(Self {
            schema: expected_schema,
            batch,
        })
    }

    pub fn schema(&self) -> StdArc<ArrowSchema> {
        self.schema.clone()
    }

    pub fn batch(&self) -> &RecordBatch {
        &self.batch
    }

    pub(crate) fn value(&self, row: usize, name: &str) -> Result<Option<RuntimeValue>, String> {
        if row >= self.batch.num_rows() {
            return Err(format!(
                "Arrow batch row {row} is outside batch with {} rows",
                self.batch.num_rows()
            ));
        }
        let column_index = match self.schema.index_of(name) {
            Ok(index) => index,
            Err(_) => return Ok(None),
        };
        let field = self.schema.field(column_index);
        runtime_value_from_arrow_array(
            self.batch.column(column_index).as_ref(),
            &parse_as_type_from_arrow(field.data_type())?,
            field.is_nullable(),
            row,
            name,
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
        let mut fields = self.schema.fields().iter().collect::<Vec<_>>();
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
            schema: self.schema.clone(),
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
                    u64::try_from(*row).map_err(|_| format!("Arrow row index {row} exceeds u64"))
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
                self.schema.clone(),
                columns,
                &RecordBatchOptions::new().with_row_count(Some(rows.len())),
            )
        } else {
            RecordBatch::try_new(self.schema.clone(), columns)
        }
        .map_err(|error| error.to_string())?;
        Ok(Self {
            schema: self.schema.clone(),
            batch,
        })
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
                    .schema
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
        Ok(Self { schema, batch })
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
        Ok(Self {
            schema: self.schema.clone(),
            batch,
        })
    }

    pub fn to_arrow_ipc_bytes(&self) -> Result<Vec<u8>, String> {
        let mut bytes = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut bytes, &self.schema)
                .map_err(|error| error.to_string())?;
            writer
                .write(&self.batch)
                .map_err(|error| error.to_string())?;
            writer.finish().map_err(|error| error.to_string())?;
        }
        Ok(bytes)
    }

    pub fn from_arrow_ipc_bytes(bytes: &[u8]) -> Result<Self, String> {
        let reader =
            StreamReader::try_new(Cursor::new(bytes), None).map_err(|error| error.to_string())?;
        let schema = reader.schema();
        let batches = reader
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| error.to_string())?;
        if batches.is_empty() {
            let batch = RecordBatch::try_new_with_options(
                schema.clone(),
                Vec::new(),
                &RecordBatchOptions::new().with_row_count(Some(0)),
            )
            .map_err(|error| error.to_string())?;
            return Ok(Self { schema, batch });
        }
        let runtime_batches = batches
            .into_iter()
            .map(|batch| Self {
                schema: schema.clone(),
                batch,
            })
            .collect::<Vec<_>>();
        let refs = runtime_batches.iter().collect::<Vec<_>>();
        Self::concat(&refs)
    }

    pub fn concat(batches: &[&Self]) -> Result<Self, String> {
        let Some(first) = batches.first() else {
            return Err("cannot concat zero arrow batches".to_string());
        };

        let schema = first.schema.clone();
        if batches
            .iter()
            .any(|batch| batch.schema.as_ref() != schema.as_ref())
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

        Ok(Self { schema, batch })
    }

    pub(crate) fn from_rows(rows: &[&RuntimeRow]) -> Result<Self, String> {
        let Some(first) = rows.first() else {
            return Err("cannot build an arrow batch from zero rows".to_string());
        };
        let schema = first.batch.schema();
        if rows
            .iter()
            .any(|row| row.batch.schema().as_ref() != schema.as_ref())
        {
            return Err("cannot build an arrow batch from rows with different schemas".to_string());
        }

        if rows.iter().all(|row| Arc::ptr_eq(&first.batch, &row.batch)) {
            if rows.len() == first.batch.batch().num_rows()
                && rows.iter().enumerate().all(|(index, row)| row.row == index)
            {
                return Ok(first.batch.as_ref().clone());
            }
            let start = first.row;
            if rows
                .iter()
                .enumerate()
                .all(|(offset, row)| row.row == start.saturating_add(offset))
            {
                return first.batch.slice(start, rows.len());
            }
            return first
                .batch
                .take(&rows.iter().map(|row| row.row).collect::<Vec<_>>());
        }

        let batches = rows
            .iter()
            .map(|row| row.one_row_batch())
            .collect::<Vec<_>>();
        Self::concat(&batches.iter().collect::<Vec<_>>())
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

    pub(crate) fn index(&self) -> usize {
        self.row
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

    pub(crate) fn one_row_batch(&self) -> RuntimeRecordBatch {
        RuntimeRecordBatch {
            schema: self.batch.schema.clone(),
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
        let mut fields = Vec::with_capacity(self.batch.schema.fields().len());
        for field in self.batch.schema.fields() {
            if let Some(value) = self.value(field.name())? {
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

    pub(crate) fn estimated_bytes(&self) -> Result<u64, String> {
        let mut bytes = 32_u64;
        for field in self.batch.schema.fields() {
            if let Some(value) = self.value(field.name())? {
                bytes = bytes
                    .saturating_add(u64::try_from(field.name().len()).unwrap_or(u64::MAX))
                    .saturating_add(value.estimated_bytes());
            }
        }
        Ok(bytes)
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
    fn next_field_index(&self) -> Result<usize, String> {
        let index = self.next_column;
        self.fields.get(index).ok_or_else(|| {
            format!(
                "Arrow batch row {} already contains all {} schema fields",
                self.rows,
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
                self.rows, field.name
            ));
        }
        append_runtime_value_to_arrow(
            self.builders[index].as_mut(),
            &field.ty,
            value,
            &format!("Arrow batch row {} field '{}'", self.rows, field.name),
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
                self.rows, field.name
            ));
        }
        append_runtime_value_to_arrow(
            self.builders[index].as_mut(),
            &field.ty,
            None,
            &format!("Arrow batch row {} field '{}'", self.rows, field.name),
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
                self.rows,
                self.next_column,
                self.fields.len()
            ));
        }
        self.rows += 1;
        self.next_column = 0;
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<RuntimeRecordBatch, String> {
        if self.next_column != 0 {
            return Err(format!(
                "Arrow batch row {} is incomplete with {} of {} schema fields",
                self.rows,
                self.next_column,
                self.fields.len()
            ));
        }
        let columns = self
            .builders
            .iter_mut()
            .map(|builder| builder.finish())
            .collect::<Vec<_>>();
        let batch = if columns.is_empty() {
            RecordBatch::try_new_with_options(
                self.schema.clone(),
                columns,
                &RecordBatchOptions::new().with_row_count(Some(self.rows)),
            )
        } else {
            RecordBatch::try_new(self.schema.clone(), columns)
        }
        .map_err(|error| error.to_string())?;
        Ok(RuntimeRecordBatch {
            schema: self.schema,
            batch,
        })
    }
}

pub(crate) fn parse_as_type_from_arrow(data_type: &ArrowDataType) -> Result<ParseAsType, String> {
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
        ArrowDataType::FixedSizeList(element, len) => Ok(ParseAsType::Array {
            element: Box::new(parse_as_type_from_arrow(element.data_type())?),
            len: u32::try_from(*len)
                .map_err(|_| format!("negative fixed-size list length {len}"))?,
        }),
        other => Err(format!(
            "runtime record materialization does not support Arrow type {other:?}"
        )),
    }
}

pub(crate) fn runtime_value_arrow_array(
    data_type: &ArrowDataType,
    value: Option<&RuntimeValue>,
    len: usize,
) -> Result<ArrayRef, String> {
    let ty = parse_as_type_from_arrow(data_type)?;
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
                    .expect("remote runtime values must contain valid rfc3339 strings"),
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
                    .expect("remote runtime values must contain valid rfc3339 strings"),
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
            Self::F32(v) => JsonValue::Number(
                JsonNumber::from_f64(v.into_inner() as f64)
                    .expect("finite f32 must map to json number"),
            ),
            Self::F64(v) => JsonValue::Number(
                JsonNumber::from_f64(v.into_inner()).expect("finite f64 must map to json number"),
            ),
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
    wire_schema: Option<&WireSchemaDefinition>,
) -> Result<Arc<CompiledCodec>, CodecError> {
    compile_codec_with_protobuf(codec, schema, wire_schema, None)
}

pub fn compile_codec_with_protobuf(
    codec: &CreateCodec,
    schema: Arc<CompiledSchema>,
    wire_schema: Option<&WireSchemaDefinition>,
    protobuf_descriptor: Option<MessageDescriptor>,
) -> Result<Arc<CompiledCodec>, CodecError> {
    let wire_schema = match (&codec.wire_format, wire_schema) {
        (CodecWireFormat::Json, Some(WireSchemaDefinition::Json(schema_def))) => {
            CompiledWireSchema::Json(compile_json_wire_schema(schema_def))
        }
        (CodecWireFormat::Cbor, Some(WireSchemaDefinition::Cbor(schema_def))) => {
            CompiledWireSchema::Cbor(compile_json_wire_schema(schema_def))
        }
        (CodecWireFormat::Avro, Some(WireSchemaDefinition::Avro(schema_def))) => {
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
        (
            CodecWireFormat::JaqNative {
                format,
                transformations,
            },
            None,
        ) => {
            if !transformations.has_any() {
                return Err(CodecError::InvalidCodec {
                    codec: codec.name.as_str().to_string(),
                    reason: "JAQ-native codec must declare a JAQ transformation".to_string(),
                });
            }
            CompiledWireSchema::JaqNative(CompiledJaqNativeCodec {
                format: JaqNativeFormat::from(*format),
                transformations: CompiledJaqTransformations::compile(codec, transformations)?,
            })
        }
        (CodecWireFormat::Protobuf(config), None) => {
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
        (CodecWireFormat::Syslog, None) => {
            syslog::validate_compiled_schema(codec, &schema)?;
            CompiledWireSchema::Syslog
        }
        (CodecWireFormat::Json, Some(WireSchemaDefinition::Avro(_))) => {
            return Err(CodecError::InvalidCodec {
                codec: codec.name.as_str().to_string(),
                reason: "codec declares JSON wire format but references an avro wire schema"
                    .to_string(),
            });
        }
        (CodecWireFormat::Json, Some(WireSchemaDefinition::Cbor(_))) => {
            return Err(CodecError::InvalidCodec {
                codec: codec.name.as_str().to_string(),
                reason: "codec declares JSON wire format but references a cbor wire schema"
                    .to_string(),
            });
        }
        (CodecWireFormat::Cbor, Some(WireSchemaDefinition::Json(_))) => {
            return Err(CodecError::InvalidCodec {
                codec: codec.name.as_str().to_string(),
                reason: "codec declares CBOR wire format but references a json wire schema"
                    .to_string(),
            });
        }
        (CodecWireFormat::Cbor, Some(WireSchemaDefinition::Avro(_))) => {
            return Err(CodecError::InvalidCodec {
                codec: codec.name.as_str().to_string(),
                reason: "codec declares CBOR wire format but references an avro wire schema"
                    .to_string(),
            });
        }
        (CodecWireFormat::Avro, Some(WireSchemaDefinition::Json(_))) => {
            return Err(CodecError::InvalidCodec {
                codec: codec.name.as_str().to_string(),
                reason: "codec declares AVRO wire format but references a json wire schema"
                    .to_string(),
            });
        }
        (CodecWireFormat::Avro, Some(WireSchemaDefinition::Cbor(_))) => {
            return Err(CodecError::InvalidCodec {
                codec: codec.name.as_str().to_string(),
                reason: "codec declares AVRO wire format but references a cbor wire schema"
                    .to_string(),
            });
        }
        (CodecWireFormat::Json, None) => {
            return Err(CodecError::InvalidCodec {
                codec: codec.name.as_str().to_string(),
                reason: "codec declares JSON wire format but has no wire schema".to_string(),
            });
        }
        (CodecWireFormat::Cbor, None) => {
            return Err(CodecError::InvalidCodec {
                codec: codec.name.as_str().to_string(),
                reason: "codec declares CBOR wire format but has no wire schema".to_string(),
            });
        }
        (CodecWireFormat::Avro, None) => {
            return Err(CodecError::InvalidCodec {
                codec: codec.name.as_str().to_string(),
                reason: "codec declares AVRO wire format but has no wire schema".to_string(),
            });
        }
        (CodecWireFormat::JaqNative { .. }, Some(_)) => {
            return Err(CodecError::InvalidCodec {
                codec: codec.name.as_str().to_string(),
                reason: "JAQ-native codec must not reference a wire schema".to_string(),
            });
        }
        (CodecWireFormat::Protobuf(_), Some(_)) => {
            return Err(CodecError::InvalidCodec {
                codec: codec.name.as_str().to_string(),
                reason: "protobuf codec must not reference a wire schema".to_string(),
            });
        }
        (CodecWireFormat::Syslog, Some(_)) => {
            return Err(CodecError::InvalidCodec {
                codec: codec.name.as_str().to_string(),
                reason: "SYSLOG codec must not reference a wire schema".to_string(),
            });
        }
    };

    Ok(Arc::new(CompiledCodec {
        name: codec.name.clone(),
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

pub fn decode_with_codec(
    codec: &CompiledCodec,
    payload: &[u8],
) -> Result<RuntimeRecordBatch, CodecError> {
    match &codec.wire_schema {
        CompiledWireSchema::Json(wire_schema) => decode_json(codec, wire_schema, payload),
        CompiledWireSchema::Cbor(wire_schema) => decode_cbor(codec, wire_schema, payload),
        CompiledWireSchema::Avro(wire_schema) => decode_avro(codec, wire_schema, payload),
        CompiledWireSchema::JaqNative(native) => decode_jaq_native(codec, native, payload),
        CompiledWireSchema::Protobuf(protobuf) => decode_protobuf(codec, protobuf, payload),
        CompiledWireSchema::Syslog => syslog::decode(codec, payload),
    }
}

pub(crate) fn decode_with_codec_owned(
    codec: &CompiledCodec,
    mut payload: Vec<u8>,
) -> Result<RuntimeRecordBatch, CodecError> {
    match &codec.wire_schema {
        CompiledWireSchema::Json(wire_schema) => decode_json_mut(codec, wire_schema, &mut payload),
        CompiledWireSchema::Cbor(wire_schema) => decode_cbor(codec, wire_schema, &payload),
        CompiledWireSchema::Avro(wire_schema) => decode_avro(codec, wire_schema, &payload),
        CompiledWireSchema::JaqNative(native) => decode_jaq_native(codec, native, &payload),
        CompiledWireSchema::Protobuf(protobuf) => decode_protobuf(codec, protobuf, &payload),
        CompiledWireSchema::Syslog => syslog::decode(codec, &payload),
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
                if array.value_length() != i32::try_from(*len).unwrap_or(i32::MAX) {
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
                let end = start.saturating_add(*len as usize);
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
    payload: &[u8],
) -> Result<RuntimeRecordBatch, CodecError> {
    let value =
        serde_json::from_slice::<JsonValue>(payload).map_err(|source| CodecError::JsonDecode {
            codec: codec.name.as_str().to_string(),
            source,
        })?;
    decode_json_payload(codec, wire_schema, value)
}

fn decode_json_mut(
    codec: &CompiledCodec,
    wire_schema: &CompiledJsonWireSchema,
    payload: &mut [u8],
) -> Result<RuntimeRecordBatch, CodecError> {
    let value = simd_json::from_slice::<JsonValue>(payload).map_err(|source| {
        CodecError::SimdJsonDecode {
            codec: codec.name.as_str().to_string(),
            source,
        }
    })?;
    decode_json_payload(codec, wire_schema, value)
}

fn decode_json_payload(
    codec: &CompiledCodec,
    wire_schema: &CompiledJsonWireSchema,
    value: JsonValue,
) -> Result<RuntimeRecordBatch, CodecError> {
    decode_json_value(codec, &value, Some(wire_schema))
}

fn decode_cbor(
    codec: &CompiledCodec,
    wire_schema: &CompiledJsonWireSchema,
    payload: &[u8],
) -> Result<RuntimeRecordBatch, CodecError> {
    let value = ciborium::from_reader::<JsonValue, _>(Cursor::new(payload)).map_err(|source| {
        CodecError::CborDecode {
            codec: codec.name.as_str().to_string(),
            reason: source.to_string(),
        }
    })?;
    decode_json_payload(codec, wire_schema, value)
}

fn decode_jaq_native(
    codec: &CompiledCodec,
    native: &CompiledJaqNativeCodec,
    payload: &[u8],
) -> Result<RuntimeRecordBatch, CodecError> {
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
    let value = run_jaq_transformation(codec, program, value)?;
    decode_json_value(codec, &value, None)
}

fn decode_protobuf(
    codec: &CompiledCodec,
    protobuf: &CompiledProtobufCodec,
    payload: &[u8],
) -> Result<RuntimeRecordBatch, CodecError> {
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
    let value = run_jaq_transformation(codec, program, value)?;
    decode_json_value(codec, &value, None)
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
) -> Result<RuntimeRecordBatch, CodecError> {
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

    let mut builder = codec.schema.batch_builder(1);
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
    builder
        .finish_row()
        .and_then(|()| builder.finish())
        .map_err(|reason| CodecError::InvalidCodec {
            codec: codec.name.as_str().to_string(),
            reason,
        })
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
) -> Result<RuntimeRecordBatch, CodecError> {
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

    let mut builder = codec.schema.batch_builder(1);
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
    builder
        .finish_row()
        .and_then(|()| builder.finish())
        .map_err(|reason| CodecError::InvalidCodec {
            codec: codec.name.as_str().to_string(),
            reason,
        })
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
            value
                .as_str()
                .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
                .and_then(|value| value.timestamp_nanos_opt())
        ),
        ParseAsType::F32 => {
            append_primitive!(Float32Builder, value.as_f64().map(|value| value as f32))
        }
        ParseAsType::F64 => append_primitive!(Float64Builder, value.as_f64()),
        ParseAsType::Array { element, len } => {
            let values = value.as_array().ok_or_else(&incompatible)?;
            if values.len() != *len as usize {
                return Err(incompatible());
            }
            let builder = typed_arrow_builder::<FixedSizeListBuilder<Box<dyn ArrayBuilder>>>(
                builder, context,
            )?;
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
                DateTime::parse_from_rfc3339(value)
                    .ok()
                    .and_then(|value| value.timestamp_nanos_opt())
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
            if values.len() != *len as usize {
                return Err(incompatible());
            }
            let builder = typed_arrow_builder::<FixedSizeListBuilder<Box<dyn ArrayBuilder>>>(
                builder, context,
            )?;
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
            i32::try_from(*len).expect("array length must fit Arrow fixed-size list"),
        ),
        ParseAsType::Vec { element } => ArrowDataType::List(ArrowFieldRef::new(ArrowField::new(
            "item",
            arrow_data_type(element),
            false,
        ))),
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
                Some(RuntimeValue::Array(values)) if values.len() == *len as usize => Some(values),
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
            for index in 0..*len as usize {
                append_runtime_value_to_arrow(
                    builder.values().as_mut(),
                    element,
                    values.map(|values| &values[index]),
                    &format!("{context}[{index}]"),
                )?;
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
            if array.value_length() != i32::try_from(*len).unwrap_or(i32::MAX) {
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
        AvroValue::Int(v) => Some(*v as i64),
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
        CodecJaqFormat, CodecJaqTransformations, CodecProtobufConfig, CreateCodec, CreateSchema,
        CreateWireSchema, Identifier, SchemaField,
    };

    use super::*;

    fn identifier(raw: &str) -> Identifier {
        Identifier::try_from(raw).expect("valid identifier")
    }

    fn schema() -> CreateSchema {
        CreateSchema {
            name: identifier("notification"),
            fields: vec![
                SchemaField {
                    name: identifier("user_id"),
                    ty: ParseAsType::U32,
                    optional: false,
                    sensitive: false,
                },
                SchemaField {
                    name: identifier("tenant"),
                    ty: ParseAsType::String,
                    optional: false,
                    sensitive: false,
                },
                SchemaField {
                    name: identifier("created_at"),
                    ty: ParseAsType::Datetime,
                    optional: false,
                    sensitive: false,
                },
                SchemaField {
                    name: identifier("latency"),
                    ty: ParseAsType::F64,
                    optional: false,
                    sensitive: false,
                },
                SchemaField {
                    name: identifier("active"),
                    ty: ParseAsType::Bool,
                    optional: false,
                    sensitive: false,
                },
            ],
        }
    }

    fn json_wire_schema() -> WireSchemaDefinition {
        WireSchemaDefinition::Json(CreateWireSchema {
            name: identifier("notification_wire"),
            strictness: Default::default(),
            fields: vec![
                WireSchemaField {
                    name: identifier("user_id"),
                    ty: JsonType::Integer,
                    optional: false,
                },
                WireSchemaField {
                    name: identifier("tenant"),
                    ty: JsonType::String,
                    optional: false,
                },
                WireSchemaField {
                    name: identifier("created_at"),
                    ty: JsonType::String,
                    optional: false,
                },
                WireSchemaField {
                    name: identifier("latency"),
                    ty: JsonType::Number,
                    optional: false,
                },
                WireSchemaField {
                    name: identifier("active"),
                    ty: JsonType::Boolean,
                    optional: false,
                },
            ],
        })
    }

    fn json_wire_schema_with_strictness(strictness: WireSchemaStrictness) -> WireSchemaDefinition {
        let mut wire_schema = json_wire_schema();
        let WireSchemaDefinition::Json(schema) = &mut wire_schema else {
            unreachable!("json_wire_schema returns JSON");
        };
        schema.strictness = strictness;
        wire_schema
    }

    fn cbor_wire_schema(strictness: WireSchemaStrictness) -> WireSchemaDefinition {
        WireSchemaDefinition::Cbor(CreateWireSchema {
            name: identifier("notification_wire"),
            strictness,
            fields: vec![
                WireSchemaField {
                    name: identifier("user_id"),
                    ty: JsonType::Integer,
                    optional: false,
                },
                WireSchemaField {
                    name: identifier("tenant"),
                    ty: JsonType::String,
                    optional: false,
                },
                WireSchemaField {
                    name: identifier("created_at"),
                    ty: JsonType::String,
                    optional: false,
                },
                WireSchemaField {
                    name: identifier("latency"),
                    ty: JsonType::Number,
                    optional: false,
                },
                WireSchemaField {
                    name: identifier("active"),
                    ty: JsonType::Boolean,
                    optional: false,
                },
            ],
        })
    }

    fn avro_wire_schema() -> WireSchemaDefinition {
        WireSchemaDefinition::Avro(CreateWireSchema {
            name: identifier("notification_avro"),
            strictness: Default::default(),
            fields: vec![
                WireSchemaField {
                    name: identifier("user_id"),
                    ty: AvroType::Long,
                    optional: false,
                },
                WireSchemaField {
                    name: identifier("tenant"),
                    ty: AvroType::String,
                    optional: false,
                },
                WireSchemaField {
                    name: identifier("created_at"),
                    ty: AvroType::String,
                    optional: false,
                },
                WireSchemaField {
                    name: identifier("latency"),
                    ty: AvroType::Double,
                    optional: false,
                },
                WireSchemaField {
                    name: identifier("active"),
                    ty: AvroType::Boolean,
                    optional: false,
                },
            ],
        })
    }

    fn optional_schema() -> CreateSchema {
        CreateSchema {
            name: identifier("optional_notification"),
            fields: vec![
                SchemaField {
                    name: identifier("user_id"),
                    ty: ParseAsType::U32,
                    optional: false,
                    sensitive: false,
                },
                SchemaField {
                    name: identifier("nickname"),
                    ty: ParseAsType::String,
                    optional: true,
                    sensitive: false,
                },
            ],
        }
    }

    fn optional_json_wire_schema() -> WireSchemaDefinition {
        WireSchemaDefinition::Json(CreateWireSchema {
            name: identifier("optional_notification_wire"),
            strictness: Default::default(),
            fields: vec![
                WireSchemaField {
                    name: identifier("user_id"),
                    ty: JsonType::Integer,
                    optional: false,
                },
                WireSchemaField {
                    name: identifier("nickname"),
                    ty: JsonType::String,
                    optional: true,
                },
            ],
        })
    }

    fn optional_avro_wire_schema() -> WireSchemaDefinition {
        WireSchemaDefinition::Avro(CreateWireSchema {
            name: identifier("optional_notification_avro"),
            strictness: Default::default(),
            fields: vec![
                WireSchemaField {
                    name: identifier("user_id"),
                    ty: AvroType::Long,
                    optional: false,
                },
                WireSchemaField {
                    name: identifier("nickname"),
                    ty: AvroType::String,
                    optional: true,
                },
            ],
        })
    }

    fn array_schema() -> CreateSchema {
        CreateSchema {
            name: identifier("metrics"),
            fields: vec![
                SchemaField {
                    name: identifier("cpu_last_64"),
                    ty: ParseAsType::Array {
                        element: Box::new(ParseAsType::F32),
                        len: 3,
                    },
                    optional: false,
                    sensitive: false,
                },
                SchemaField {
                    name: identifier("labels"),
                    ty: ParseAsType::Vec {
                        element: Box::new(ParseAsType::String),
                    },
                    optional: true,
                    sensitive: false,
                },
            ],
        }
    }

    fn array_json_wire_schema() -> WireSchemaDefinition {
        WireSchemaDefinition::Json(CreateWireSchema {
            name: identifier("metrics_json"),
            strictness: Default::default(),
            fields: vec![
                WireSchemaField {
                    name: identifier("cpu_last_64"),
                    ty: JsonType::Array,
                    optional: false,
                },
                WireSchemaField {
                    name: identifier("labels"),
                    ty: JsonType::Array,
                    optional: true,
                },
            ],
        })
    }

    fn array_avro_wire_schema() -> WireSchemaDefinition {
        WireSchemaDefinition::Avro(CreateWireSchema {
            name: identifier("metrics_avro"),
            strictness: Default::default(),
            fields: vec![
                WireSchemaField {
                    name: identifier("cpu_last_64"),
                    ty: AvroType::Array,
                    optional: false,
                },
                WireSchemaField {
                    name: identifier("labels"),
                    ty: AvroType::Array,
                    optional: true,
                },
            ],
        })
    }

    fn array_codec(name: &str) -> CreateCodec {
        CreateCodec {
            name: identifier(name),
            wire_format: if name.contains("avro") {
                CodecWireFormat::Avro
            } else {
                CodecWireFormat::Json
            },
            wire_schema: Some(identifier("metrics_wire")),
            schema: identifier("metrics"),
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
            name: identifier("shaped_metrics"),
            fields: vec![
                SchemaField {
                    name: identifier("matrix"),
                    ty: ParseAsType::Array {
                        len: 2,
                        element: Box::new(ParseAsType::Array {
                            len: 3,
                            element: Box::new(ParseAsType::F32),
                        }),
                    },
                    optional: false,
                    sensitive: false,
                },
                SchemaField {
                    name: identifier("samples"),
                    ty: ParseAsType::Vec {
                        element: Box::new(ParseAsType::Array {
                            len: 2,
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

    fn multidimensional_avro_wire_schema() -> WireSchemaDefinition {
        WireSchemaDefinition::Avro(CreateWireSchema {
            name: identifier("shaped_metrics_wire"),
            strictness: Default::default(),
            fields: ["matrix", "samples"]
                .into_iter()
                .map(|name| WireSchemaField {
                    name: identifier(name),
                    ty: AvroType::Array,
                    optional: false,
                })
                .collect(),
        })
    }

    fn primitive_array_cases() -> Vec<(&'static str, ParseAsType, Vec<RuntimeValue>, Vec<JsonValue>)>
    {
        vec![
            (
                "u8",
                ParseAsType::U8,
                vec![RuntimeValue::U8(1), RuntimeValue::U8(2)],
                vec![JsonValue::from(1), JsonValue::from(2)],
            ),
            (
                "i8",
                ParseAsType::I8,
                vec![RuntimeValue::I8(-1), RuntimeValue::I8(2)],
                vec![JsonValue::from(-1), JsonValue::from(2)],
            ),
            (
                "u16",
                ParseAsType::U16,
                vec![RuntimeValue::U16(10), RuntimeValue::U16(20)],
                vec![JsonValue::from(10), JsonValue::from(20)],
            ),
            (
                "i16",
                ParseAsType::I16,
                vec![RuntimeValue::I16(-10), RuntimeValue::I16(20)],
                vec![JsonValue::from(-10), JsonValue::from(20)],
            ),
            (
                "u32",
                ParseAsType::U32,
                vec![RuntimeValue::U32(100), RuntimeValue::U32(200)],
                vec![JsonValue::from(100), JsonValue::from(200)],
            ),
            (
                "i32",
                ParseAsType::I32,
                vec![RuntimeValue::I32(-100), RuntimeValue::I32(200)],
                vec![JsonValue::from(-100), JsonValue::from(200)],
            ),
            (
                "u64",
                ParseAsType::U64,
                vec![RuntimeValue::U64(1000), RuntimeValue::U64(2000)],
                vec![JsonValue::from(1000), JsonValue::from(2000)],
            ),
            (
                "i64",
                ParseAsType::I64,
                vec![RuntimeValue::I64(-1000), RuntimeValue::I64(2000)],
                vec![JsonValue::from(-1000), JsonValue::from(2000)],
            ),
            (
                "bool",
                ParseAsType::Bool,
                vec![RuntimeValue::Bool(true), RuntimeValue::Bool(false)],
                vec![JsonValue::from(true), JsonValue::from(false)],
            ),
            (
                "string",
                ParseAsType::String,
                vec![
                    RuntimeValue::String("prod".to_string()),
                    RuntimeValue::String("api".to_string()),
                ],
                vec![JsonValue::from("prod"), JsonValue::from("api")],
            ),
            (
                "datetime",
                ParseAsType::Datetime,
                vec![
                    RuntimeValue::Datetime(
                        DateTime::parse_from_rfc3339("2025-01-02T03:04:05Z")
                            .expect("valid timestamp"),
                    ),
                    RuntimeValue::Datetime(
                        DateTime::parse_from_rfc3339("2025-01-02T03:04:06Z")
                            .expect("valid timestamp"),
                    ),
                ],
                vec![
                    JsonValue::from("2025-01-02T03:04:05Z"),
                    JsonValue::from("2025-01-02T03:04:06Z"),
                ],
            ),
            (
                "f32",
                ParseAsType::F32,
                vec![
                    RuntimeValue::F32(OrderedFloat(1.25)),
                    RuntimeValue::F32(OrderedFloat(2.5)),
                ],
                vec![JsonValue::from(1.25), JsonValue::from(2.5)],
            ),
            (
                "f64",
                ParseAsType::F64,
                vec![
                    RuntimeValue::F64(OrderedFloat(10.25)),
                    RuntimeValue::F64(OrderedFloat(20.5)),
                ],
                vec![JsonValue::from(10.25), JsonValue::from(20.5)],
            ),
        ]
    }

    fn primitive_arrays_schema() -> CreateSchema {
        let mut fields = Vec::new();
        for (name, ty, _, _) in primitive_array_cases() {
            fields.push(SchemaField {
                name: identifier(&format!("{name}_array")),
                ty: ParseAsType::Array {
                    element: Box::new(ty.clone()),
                    len: 2,
                },
                optional: false,
                sensitive: false,
            });
            fields.push(SchemaField {
                name: identifier(&format!("{name}_vec")),
                ty: ParseAsType::Vec {
                    element: Box::new(ty),
                },
                optional: false,
                sensitive: false,
            });
        }
        CreateSchema {
            name: identifier("primitive_arrays"),
            fields,
        }
    }

    fn primitive_arrays_json_wire_schema() -> WireSchemaDefinition {
        WireSchemaDefinition::Json(CreateWireSchema {
            name: identifier("primitive_arrays_wire"),
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
        })
    }

    fn primitive_arrays_avro_wire_schema() -> WireSchemaDefinition {
        WireSchemaDefinition::Avro(CreateWireSchema {
            name: identifier("primitive_arrays_wire"),
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
        })
    }

    fn primitive_arrays_codec(name: &str) -> CreateCodec {
        CreateCodec {
            name: identifier(name),
            wire_format: if name.contains("avro") {
                CodecWireFormat::Avro
            } else {
                CodecWireFormat::Json
            },
            wire_schema: Some(identifier("primitive_arrays_wire")),
            schema: identifier("primitive_arrays"),
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
            name: identifier(name),
            wire_format: CodecWireFormat::JaqNative {
                format,
                transformations: CodecJaqTransformations {
                    on_ingestion: on_ingestion.map(str::to_string),
                    on_emitting: on_emitting.map(str::to_string),
                },
            },
            wire_schema: None,
            schema: identifier(schema),
            encoding_rules: Vec::new(),
        }
    }

    fn jaq_native_identity_codec(name: &str, format: CodecJaqFormat, schema: &str) -> CreateCodec {
        jaq_native_codec(name, format, schema, Some("."), Some("."))
    }

    fn protobuf_schema() -> CreateSchema {
        CreateSchema {
            name: identifier("protobuf_notification"),
            fields: vec![
                SchemaField {
                    name: identifier("user_id"),
                    ty: ParseAsType::U32,
                    optional: false,
                    sensitive: false,
                },
                SchemaField {
                    name: identifier("tenant"),
                    ty: ParseAsType::String,
                    optional: false,
                    sensitive: false,
                },
                SchemaField {
                    name: identifier("payload"),
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
            name: identifier(name),
            wire_format: CodecWireFormat::Protobuf(CodecProtobufConfig {
                resource: identifier("proto_bundle"),
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
            wire_schema: None,
            schema: identifier("protobuf_notification"),
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
        for (name, _, values, _) in primitive_array_cases() {
            fields.push((format!("{name}_array"), RuntimeValue::Array(values.clone())));
            fields.push((format!("{name}_vec"), RuntimeValue::Vec(values)));
        }
        test_runtime_row(fields)
    }

    fn primitive_arrays_json_payload() -> Vec<u8> {
        let mut object = JsonMap::new();
        for (name, _, _, values) in primitive_array_cases() {
            object.insert(format!("{name}_array"), JsonValue::Array(values.clone()));
            object.insert(format!("{name}_vec"), JsonValue::Array(values));
        }
        serde_json::to_vec(&JsonValue::Object(object)).expect("valid json")
    }

    fn optional_codec(name: &str) -> CreateCodec {
        CreateCodec {
            name: identifier(name),
            wire_format: if name.contains("avro") {
                CodecWireFormat::Avro
            } else {
                CodecWireFormat::Json
            },
            wire_schema: Some(identifier("optional_notification_wire")),
            schema: identifier("optional_notification"),
            encoding_rules: Vec::new(),
        }
    }

    fn codec(name: &str) -> CreateCodec {
        CreateCodec {
            name: identifier(name),
            wire_format: if name.contains("avro") {
                CodecWireFormat::Avro
            } else {
                CodecWireFormat::Json
            },
            wire_schema: Some(identifier("notification_wire")),
            schema: identifier("notification"),
            encoding_rules: Vec::new(),
        }
    }

    fn syslog_schema() -> CreateSchema {
        CreateSchema {
            name: identifier("syslog_event"),
            fields: vec![
                SchemaField {
                    name: identifier("facility"),
                    ty: ParseAsType::U8,
                    optional: false,
                    sensitive: false,
                },
                SchemaField {
                    name: identifier("severity"),
                    ty: ParseAsType::U8,
                    optional: false,
                    sensitive: false,
                },
                SchemaField {
                    name: identifier("timestamp"),
                    ty: ParseAsType::Datetime,
                    optional: true,
                    sensitive: false,
                },
                SchemaField {
                    name: identifier("hostname"),
                    ty: ParseAsType::String,
                    optional: true,
                    sensitive: false,
                },
                SchemaField {
                    name: identifier("app_name"),
                    ty: ParseAsType::String,
                    optional: true,
                    sensitive: false,
                },
                SchemaField {
                    name: identifier("proc_id"),
                    ty: ParseAsType::String,
                    optional: true,
                    sensitive: false,
                },
                SchemaField {
                    name: identifier("msg_id"),
                    ty: ParseAsType::String,
                    optional: true,
                    sensitive: false,
                },
                SchemaField {
                    name: identifier("structured_data"),
                    ty: ParseAsType::String,
                    optional: true,
                    sensitive: false,
                },
                SchemaField {
                    name: identifier("message"),
                    ty: ParseAsType::String,
                    optional: false,
                    sensitive: false,
                },
            ],
        }
    }

    fn syslog_codec() -> CreateCodec {
        CreateCodec {
            name: identifier("syslog_codec"),
            wire_format: CodecWireFormat::Syslog,
            wire_schema: None,
            schema: identifier("syslog_event"),
            encoding_rules: Vec::new(),
        }
    }

    fn compiled_syslog_codec() -> Arc<CompiledCodec> {
        compile_codec(
            &syslog_codec(),
            Arc::new(compile_schema(&syslog_schema())),
            None,
        )
        .expect("SYSLOG codec should compile")
    }

    fn schemaful_cbor_codec(name: &str) -> CreateCodec {
        CreateCodec {
            name: identifier(name),
            wire_format: CodecWireFormat::Cbor,
            wire_schema: Some(identifier("notification_wire")),
            schema: identifier("notification"),
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

    #[test]
    fn json_codec_roundtrips_runtime_records() {
        let compiled_schema = Arc::new(compile_schema(&schema()));
        let compiled_codec = compile_codec(
            &codec("json_codec"),
            compiled_schema,
            Some(&json_wire_schema()),
        )
        .expect("codec should compile");
        let payload = encode_arrow_record(&compiled_codec, &record()).expect("must encode");
        let decoded = decode_with_codec(&compiled_codec, &payload).expect("must decode");

        assert_eq!(
            single_batch_value(&decoded, "user_id"),
            Some(RuntimeValue::U32(42))
        );
        assert_eq!(
            single_batch_value(&decoded, "tenant"),
            Some(RuntimeValue::String("acme".to_string()))
        );
        assert_eq!(
            single_batch_value(&decoded, "active"),
            Some(RuntimeValue::Bool(true))
        );
    }

    #[test]
    fn syslog_codec_decodes_rfc5424_directly_into_arrow() {
        let codec = compiled_syslog_codec();
        let payload = b"<34>1 2003-10-11T22:14:15.003Z edge-1 orders 123 ID47 \
                        [exampleSDID@32473 iut=\"3\" note=\"a\\]b\"] order accepted\r\n\0";
        let decoded = decode_with_codec(&codec, payload).expect("RFC 5424 should decode");

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
        let decoded = decode_with_codec(
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

        let defaulted = decode_with_codec(&codec, b"plain syslog message")
            .expect("message without PRI should decode");
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
        let malformed_priority = decode_with_codec(&codec, b"<013>plain syslog message")
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

        let tagged = decode_with_codec(
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

        let bom = decode_with_codec(
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
                decode_with_codec(&codec, payload.as_bytes()).is_err(),
                "RFC 5424 timestamp '{timestamp}' must be rejected"
            );
        }
    }

    #[test]
    fn syslog_codec_accepts_rfc5424_control_values_while_preserving_structured_data() {
        let codec = compiled_syslog_codec();
        let payload = b"<34>1 - edge app 1 ID [example@32473 note=\"line\tvalue\"] body";
        let decoded = decode_with_codec(&codec, payload)
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
            name: identifier("syslog_minimal"),
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
        let codec = compile_codec(&codec_model, Arc::new(compile_schema(&schema_model)), None)
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
            Some(&json_wire_schema()),
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
            let decoded = decode_with_codec(&compiled_codec, payload)
                .expect("columnar JSON payload should decode");
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
            Some(&json_wire_schema()),
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
        let decoded = decode_with_codec(&compiled_codec, &payload)
            .expect("reused payload should contain only the second row");
        assert_eq!(
            single_batch_value(&decoded, "tenant"),
            Some(RuntimeValue::String("b".to_string()))
        );
    }

    #[test]
    fn codec_batch_encoder_does_not_eagerly_encode_arrow_rows() {
        let schema_model = CreateSchema {
            name: identifier("counter"),
            fields: vec![SchemaField {
                name: identifier("value"),
                ty: ParseAsType::U64,
                optional: false,
                sensitive: false,
            }],
        };
        let compiled_schema = Arc::new(compile_schema(&schema_model));
        let codec_model = CreateCodec {
            name: identifier("counter_avro"),
            wire_format: CodecWireFormat::Avro,
            wire_schema: Some(identifier("counter_wire")),
            schema: schema_model.name.clone(),
            encoding_rules: Vec::new(),
        };
        let wire_schema = WireSchemaDefinition::Avro(CreateWireSchema {
            name: identifier("counter_wire"),
            strictness: Default::default(),
            fields: vec![WireSchemaField {
                name: identifier("value"),
                ty: AvroType::Long,
                optional: false,
            }],
        });
        let compiled_codec =
            compile_codec(&codec_model, compiled_schema.clone(), Some(&wire_schema))
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
    fn avro_codec_roundtrips_runtime_records() {
        let compiled_schema = Arc::new(compile_schema(&schema()));
        let compiled_codec = compile_codec(
            &codec("avro_codec"),
            compiled_schema,
            Some(&avro_wire_schema()),
        )
        .expect("codec should compile");
        let payload = encode_arrow_record(&compiled_codec, &record()).expect("must encode");
        let decoded = decode_with_codec(&compiled_codec, &payload).expect("must decode");

        assert_eq!(
            single_batch_value(&decoded, "user_id"),
            Some(RuntimeValue::U32(42))
        );
        assert_eq!(
            single_batch_value(&decoded, "tenant"),
            Some(RuntimeValue::String("acme".to_string()))
        );
        assert_eq!(
            single_batch_value(&decoded, "latency"),
            Some(RuntimeValue::F64(OrderedFloat(12.5)))
        );
    }

    #[test]
    fn avro_codec_decodes_wire_fields_into_internal_arrow_order() {
        let compiled_schema = Arc::new(compile_schema(&schema()));
        let mut wire_schema = avro_wire_schema();
        let WireSchemaDefinition::Avro(wire_schema_model) = &mut wire_schema else {
            unreachable!("avro_wire_schema returns AVRO");
        };
        wire_schema_model.fields.rotate_left(2);
        let compiled_codec =
            compile_codec(&codec("avro_codec"), compiled_schema, Some(&wire_schema))
                .expect("codec should compile");

        let payload = encode_arrow_record(&compiled_codec, &record()).expect("must encode");
        let decoded = decode_with_codec(&compiled_codec, &payload).expect("must decode");

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
    fn schemaful_cbor_codec_roundtrips_runtime_records_without_jaq() {
        let compiled_schema = Arc::new(compile_schema(&schema()));
        let compiled_codec = compile_codec(
            &schemaful_cbor_codec("schemaful_cbor_codec"),
            compiled_schema,
            Some(&cbor_wire_schema(WireSchemaStrictness::Strict)),
        )
        .expect("codec should compile");
        let payload = encode_arrow_record(&compiled_codec, &record()).expect("must encode");
        let decoded = decode_with_codec(&compiled_codec, &payload).expect("must decode");

        assert_eq!(
            single_batch_value(&decoded, "user_id"),
            Some(RuntimeValue::U32(42))
        );
        assert_eq!(
            single_batch_value(&decoded, "tenant"),
            Some(RuntimeValue::String("acme".to_string()))
        );
        assert_eq!(
            single_batch_value(&decoded, "active"),
            Some(RuntimeValue::Bool(true))
        );
    }

    #[test]
    fn json_codec_and_arrow_support_array_and_vector_fields() {
        let compiled_schema = Arc::new(compile_schema(&array_schema()));
        let compiled_codec = compile_codec(
            &array_codec("json_array_codec"),
            compiled_schema.clone(),
            Some(&array_json_wire_schema()),
        )
        .expect("codec should compile");

        let decoded = decode_with_codec(
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
            name: identifier("shaped_metrics_codec"),
            wire_format: CodecWireFormat::Avro,
            wire_schema: Some(identifier("shaped_metrics_wire")),
            schema: identifier("shaped_metrics"),
            encoding_rules: Vec::new(),
        };
        let codec = compile_codec(&codec, schema, Some(&multidimensional_avro_wire_schema()))
            .expect("multidimensional Avro codec should compile");
        let expected = multidimensional_array_record();

        let payload = encode_arrow_record(&codec, &expected).expect("must encode nested arrays");
        let decoded = decode_with_codec(&codec, &payload).expect("must decode nested arrays");

        for field in codec.schema.fields() {
            assert_eq!(
                single_batch_value(&decoded, &field.name),
                row_value(&expected, &field.name)
            );
        }
    }

    #[test]
    fn avro_codec_supports_array_and_vector_fields() {
        let compiled_schema = Arc::new(compile_schema(&array_schema()));
        let compiled_codec = compile_codec(
            &array_codec("avro_array_codec"),
            compiled_schema,
            Some(&array_avro_wire_schema()),
        )
        .expect("codec should compile");

        let payload = encode_arrow_record(&compiled_codec, &array_record()).expect("must encode");
        let decoded = decode_with_codec(&compiled_codec, &payload).expect("must decode");

        assert_eq!(
            single_batch_value(&decoded, "cpu_last_64"),
            row_value(&array_record(), "cpu_last_64")
        );
        assert_eq!(
            single_batch_value(&decoded, "labels"),
            row_value(&array_record(), "labels")
        );
    }

    #[test]
    fn cbor_codec_supports_array_and_vector_fields() {
        let compiled_schema = Arc::new(compile_schema(&array_schema()));
        let compiled_codec = compile_codec(
            &jaq_native_identity_codec("cbor_array_codec", CodecJaqFormat::Cbor, "metrics"),
            compiled_schema,
            None,
        )
        .expect("codec should compile");

        let payload = encode_arrow_record(&compiled_codec, &array_record()).expect("must encode");
        let decoded = decode_with_codec(&compiled_codec, &payload).expect("must decode");

        assert_eq!(
            single_batch_value(&decoded, "cpu_last_64"),
            row_value(&array_record(), "cpu_last_64")
        );
        assert_eq!(
            single_batch_value(&decoded, "labels"),
            row_value(&array_record(), "labels")
        );
    }

    #[test]
    fn yaml_codec_supports_array_and_vector_fields() {
        let compiled_schema = Arc::new(compile_schema(&array_schema()));
        let compiled_codec = compile_codec(
            &jaq_native_identity_codec("yaml_array_codec", CodecJaqFormat::Yaml, "metrics"),
            compiled_schema,
            None,
        )
        .expect("codec should compile");

        let payload = encode_arrow_record(&compiled_codec, &array_record()).expect("must encode");
        let decoded = decode_with_codec(&compiled_codec, &payload).expect("must decode");

        assert_eq!(
            single_batch_value(&decoded, "cpu_last_64"),
            row_value(&array_record(), "cpu_last_64")
        );
        assert_eq!(
            single_batch_value(&decoded, "labels"),
            row_value(&array_record(), "labels")
        );
    }

    #[test]
    fn json_codec_and_arrow_support_arrays_and_vectors_for_all_primitive_types() {
        let expected = primitive_arrays_record();
        let compiled_schema = Arc::new(compile_schema(&primitive_arrays_schema()));
        let compiled_codec = compile_codec(
            &primitive_arrays_codec("json_primitive_arrays_codec"),
            compiled_schema.clone(),
            Some(&primitive_arrays_json_wire_schema()),
        )
        .expect("codec should compile");

        let decoded = decode_with_codec(&compiled_codec, &primitive_arrays_json_payload())
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

    #[test]
    fn cbor_codec_supports_arrays_and_vectors_for_all_primitive_types() {
        let expected = primitive_arrays_record();
        let compiled_schema = Arc::new(compile_schema(&primitive_arrays_schema()));
        let compiled_codec = compile_codec(
            &jaq_native_identity_codec(
                "cbor_primitive_arrays_codec",
                CodecJaqFormat::Cbor,
                "primitive_arrays",
            ),
            compiled_schema.clone(),
            None,
        )
        .expect("codec should compile");

        let payload = encode_arrow_record(&compiled_codec, &expected).expect("must encode");
        let decoded = decode_with_codec(&compiled_codec, &payload).expect("must decode");

        for field in compiled_schema.fields() {
            assert_eq!(
                single_batch_value(&decoded, &field.name),
                row_value(&expected, &field.name),
                "field {} should roundtrip through CBOR",
                field.name
            );
        }
    }

    #[test]
    fn toml_codec_supports_arrays_and_vectors_for_all_primitive_types() {
        let expected = primitive_arrays_record();
        let compiled_schema = Arc::new(compile_schema(&primitive_arrays_schema()));
        let compiled_codec = compile_codec(
            &jaq_native_identity_codec(
                "toml_primitive_arrays_codec",
                CodecJaqFormat::Toml,
                "primitive_arrays",
            ),
            compiled_schema.clone(),
            None,
        )
        .expect("codec should compile");

        let payload = encode_arrow_record(&compiled_codec, &expected).expect("must encode");
        let decoded = decode_with_codec(&compiled_codec, &payload).expect("must decode");

        for field in compiled_schema.fields() {
            assert_eq!(
                single_batch_value(&decoded, &field.name),
                row_value(&expected, &field.name),
                "field {} should roundtrip through TOML",
                field.name
            );
        }
    }

    #[test]
    fn avro_codec_supports_arrays_and_vectors_for_all_primitive_types() {
        let expected = primitive_arrays_record();
        let compiled_schema = Arc::new(compile_schema(&primitive_arrays_schema()));
        let compiled_codec = compile_codec(
            &primitive_arrays_codec("avro_primitive_arrays_codec"),
            compiled_schema.clone(),
            Some(&primitive_arrays_avro_wire_schema()),
        )
        .expect("codec should compile");

        let payload = encode_arrow_record(&compiled_codec, &expected).expect("must encode");
        let decoded = decode_with_codec(&compiled_codec, &payload).expect("must decode");

        for field in compiled_schema.fields() {
            assert_eq!(
                single_batch_value(&decoded, &field.name),
                row_value(&expected, &field.name),
                "field {} should roundtrip through Avro",
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
            Some(&json_wire_schema()),
        )
        .expect("codec should compile");

        let missing = br#"{"user_id":42,"tenant":"acme","created_at":"2025-01-02T03:04:05+00:00","active":true}"#;
        let err =
            decode_with_codec(&compiled_codec, missing).expect_err("must reject missing field");
        assert!(matches!(err, CodecError::MissingField { field, .. } if field == "latency"));

        let bad_type = br#"{"user_id":"forty-two","tenant":"acme","created_at":"2025-01-02T03:04:05+00:00","latency":12.5,"active":true}"#;
        let err = decode_with_codec(&compiled_codec, bad_type).expect_err("must reject bad type");
        assert!(matches!(err, CodecError::ParseField { field, .. } if field == "user_id"));
    }

    #[test]
    fn strict_json_wire_schema_rejects_unknown_fields() {
        let compiled_schema = Arc::new(compile_schema(&schema()));
        let compiled_codec = compile_codec(
            &codec("json_codec"),
            compiled_schema,
            Some(&json_wire_schema_with_strictness(
                WireSchemaStrictness::Strict,
            )),
        )
        .expect("codec should compile");

        let err = decode_with_codec(&compiled_codec, notification_json_payload_with_extra())
            .expect_err("strict wire schema should reject unknown fields");
        assert!(matches!(err, CodecError::UnexpectedField { field, .. } if field == "ignored"));
    }

    #[test]
    fn loose_json_wire_schema_drops_unknown_fields() {
        let compiled_schema = Arc::new(compile_schema(&schema()));
        let compiled_codec = compile_codec(
            &codec("json_codec"),
            compiled_schema,
            Some(&json_wire_schema_with_strictness(
                WireSchemaStrictness::Loose,
            )),
        )
        .expect("codec should compile");

        let decoded = decode_with_codec(&compiled_codec, notification_json_payload_with_extra())
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
            Some(&cbor_wire_schema(WireSchemaStrictness::Loose)),
        )
        .expect("codec should compile");

        let decoded = decode_with_codec(&compiled_codec, &notification_cbor_payload_with_extra())
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
            Some(&optional_json_wire_schema()),
        )
        .expect("codec should compile");

        let missing = decode_with_codec(&compiled_codec, br#"{"user_id":42}"#)
            .expect("missing optional field should decode");
        assert_eq!(
            single_batch_value(&missing, "user_id"),
            Some(RuntimeValue::U32(42))
        );
        assert_eq!(single_batch_value(&missing, "nickname"), None);

        let explicit_null = decode_with_codec(&compiled_codec, br#"{"user_id":7,"nickname":null}"#)
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
            Some(&optional_json_wire_schema()),
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
            Some(&optional_avro_wire_schema()),
        )
        .expect("codec should compile");

        let payload = encode_test_fields(
            &compiled_codec,
            [("user_id".to_string(), RuntimeValue::U32(42))],
        )
        .expect("must encode");
        let decoded = decode_with_codec(&compiled_codec, &payload).expect("must decode");

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
            Some(&json_wire_schema()),
        )
        .expect("codec should compile");

        let err =
            decode_with_codec(&compiled_codec, br#"[1,2,3]"#).expect_err("arrays must be rejected");
        assert!(matches!(err, CodecError::ExpectedObject { .. }));

        let missing_wire_schema = WireSchemaDefinition::Json(CreateWireSchema {
            name: identifier("notification_wire_partial"),
            strictness: WireSchemaStrictness::Loose,
            fields: vec![
                WireSchemaField {
                    name: identifier("user_id"),
                    ty: JsonType::Integer,
                    optional: false,
                },
                WireSchemaField {
                    name: identifier("tenant"),
                    ty: JsonType::String,
                    optional: false,
                },
            ],
        });
        let missing_wire_codec = compile_codec(
            &CreateCodec {
                name: identifier("json_partial"),
                wire_format: CodecWireFormat::Json,
                wire_schema: Some(identifier("notification_wire_partial")),
                schema: identifier("notification"),
                encoding_rules: Vec::new(),
            },
            compiled_schema,
            Some(&missing_wire_schema),
        )
        .expect("codec should compile");

        let err = decode_with_codec(
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
        let compiled_codec = compile_codec(
            &jaq_native_codec(
                "json_with_jaq",
                CodecJaqFormat::Json,
                "notification",
                Some(".payload"),
                None,
            ),
            compiled_schema,
            None,
        )
        .expect("codec should compile");

        let decoded = decode_with_codec(
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
        let compiled_codec = compile_codec(
            &jaq_native_codec(
                "json_with_emitting_jaq",
                CodecJaqFormat::Json,
                "notification",
                None,
                Some("{payload: .}"),
            ),
            compiled_schema,
            None,
        )
        .expect("codec should compile");

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
        let err = compile_codec(
            &jaq_native_codec(
                "json_with_bad_ingestion_jaq",
                CodecJaqFormat::Json,
                "notification",
                Some(". | "),
                None,
            ),
            compiled_schema,
            None,
        )
        .expect_err("invalid jaq must fail");

        assert!(matches!(err, CodecError::InvalidJaqTransformation { .. }));
    }

    #[test]
    fn jaq_native_codec_rejects_invalid_emitting_jaq_program() {
        let compiled_schema = Arc::new(compile_schema(&schema()));
        let err = compile_codec(
            &jaq_native_codec(
                "json_with_bad_emitting_jaq",
                CodecJaqFormat::Json,
                "notification",
                None,
                Some(". | "),
            ),
            compiled_schema,
            None,
        )
        .expect_err("invalid jaq must fail");

        assert!(matches!(err, CodecError::InvalidJaqTransformation { .. }));
    }

    #[test]
    fn protobuf_codec_applies_transformation_on_ingestion_before_decoding() {
        let codec = protobuf_codec("protobuf_ingest", Some("."), None);
        let compiled_schema = Arc::new(compile_schema(&protobuf_schema()));
        let compiled_codec =
            compile_codec_with_protobuf(&codec, compiled_schema, None, Some(protobuf_descriptor()))
                .expect("codec should compile");
        assert!(compiled_codec.requires_blocking_decode());
        assert!(!compiled_codec.requires_blocking_encode());

        let payload = [
            0x08, 42, 0x12, 4, b'a', b'c', b'm', b'e', 0x1a, 5, b'h', b'e', b'l', b'l', b'o',
        ];
        let decoded = decode_with_codec(&compiled_codec, &payload).expect("must decode");

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
        let compiled_codec =
            compile_codec_with_protobuf(&codec, compiled_schema, None, Some(protobuf_descriptor()))
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
        let err = compile_codec_with_protobuf(&codec, compiled_schema, None, None)
            .expect_err("descriptor is mandatory");

        assert!(
            matches!(err, CodecError::InvalidCodec { reason, .. } if reason.contains("compiled descriptor"))
        );
    }

    #[test]
    fn cbor_codec_roundtrips_runtime_records() {
        let compiled_schema = Arc::new(compile_schema(&schema()));
        let compiled_codec = compile_codec(
            &jaq_native_identity_codec("cbor_codec", CodecJaqFormat::Cbor, "notification"),
            compiled_schema,
            None,
        )
        .expect("codec should compile");
        let payload = encode_arrow_record(&compiled_codec, &record()).expect("must encode");
        let decoded = decode_with_codec(&compiled_codec, &payload).expect("must decode");

        assert_eq!(
            single_batch_value(&decoded, "user_id"),
            Some(RuntimeValue::U32(42))
        );
        assert_eq!(
            single_batch_value(&decoded, "tenant"),
            Some(RuntimeValue::String("acme".to_string()))
        );
        assert_eq!(
            single_batch_value(&decoded, "active"),
            Some(RuntimeValue::Bool(true))
        );
    }

    #[test]
    fn xml_codec_emits_runtime_records() {
        let compiled_schema = Arc::new(compile_schema(&schema()));
        let compiled_codec = compile_codec(
            &jaq_native_codec(
                "xml_codec",
                CodecJaqFormat::Xml,
                "notification",
                None,
                Some(
                    r#"{t: "notification", c: [{t: "user_id", c: [(.user_id | tostring)]}, {t: "tenant", c: [.tenant]}]}"#,
                ),
            ),
            compiled_schema,
            None,
        )
        .expect("codec should compile");
        let payload = encode_arrow_record(&compiled_codec, &record()).expect("must encode");

        assert_eq!(
            String::from_utf8(payload).expect("xml must be utf8"),
            "<notification><user_id>42</user_id><tenant>acme</tenant></notification>"
        );
    }
}
