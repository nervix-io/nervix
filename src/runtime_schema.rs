use std::{io::Cursor, str::FromStr, sync::Arc};

use ahash::{HashMap, HashMapExt};
use apache_avro::{
    Schema as AvroSchema, from_avro_datum, to_avro_datum, types::Value as AvroValue,
};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Datum, FixedSizeListArray, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, ListArray, RecordBatch, RecordBatchOptions,
    Scalar, StringArray, TimestampNanosecondArray, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array,
    builder::{
        BooleanBuilder, FixedSizeListBuilder, Float32Builder, Float64Builder, Int8Builder,
        Int16Builder, Int32Builder, Int64Builder, ListBuilder, StringBuilder,
        TimestampNanosecondBuilder, UInt8Builder, UInt16Builder, UInt32Builder, UInt64Builder,
    },
};
use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
use arrow_ord::cmp::eq as arrow_eq;
use arrow_schema::{
    DataType as ArrowDataType, Field as ArrowField, FieldRef as ArrowFieldRef,
    Schema as ArrowSchema, TimeUnit as ArrowTimeUnit,
};
use arrow_select::{concat::concat as concat_arrow_arrays, filter::filter_record_batch};
use bytes::Bytes;
use chrono::{DateTime, FixedOffset};
use jaq_core::{
    Compiler as JaqCompiler, Ctx as JaqCtx, Vars as JaqVars, data,
    load::{Arena, File, Loader},
    unwrap_valr,
};
use jaq_fmts::{
    Format as JaqFormat, read as jaq_read,
    write::{self as jaq_write, Writer as JaqWriter},
};
use jaq_json::{Num as JaqNum, Val as JaqVal};
use nervix_models::{
    AvroType, CodecJaqFormat, CodecJaqTransformations, CodecWireFormat, CreateCodec, CreateSchema,
    CreateWireSchema, CreateWireSchemaStmt, Identifier, JsonType, ParseAsType, RemoteDecodedRecord,
    RemoteRuntimeElementValue, RemoteRuntimeField, RemoteRuntimeRecord,
    RemoteRuntimeRecordMetadata, RemoteRuntimeValue, Timestamp, WireSchemaField,
};
use nervix_wasm::{WasmProcessorField, WasmProcessorSchema, WasmProcessorType};
use ordered_float::OrderedFloat;
use prost::Message as ProstMessage;
use prost_reflect::{
    DescriptorPool, DeserializeOptions as ProtobufDeserializeOptions, DynamicMessage,
    MessageDescriptor, SerializeOptions as ProtobufSerializeOptions,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};
use thiserror::Error;

#[derive(Debug, Clone)]
pub struct CompiledSchema {
    fields: Vec<CompiledSchemaField>,
    arrow_schema: Arc<ArrowSchema>,
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

#[derive(Debug, Clone)]
enum CompiledWireSchema {
    Json(CompiledJsonWireSchema),
    Avro(CompiledAvroWireSchema),
    JaqNative(CompiledJaqNativeCodec),
    Protobuf(CompiledProtobufCodec),
}

#[derive(Debug, Clone)]
struct CompiledJsonWireSchema {
    fields: HashMap<String, CompiledJsonWireField>,
}

#[derive(Debug, Clone)]
struct CompiledAvroWireSchema {
    fields: HashMap<String, CompiledAvroWireField>,
    schema: AvroSchema,
}

#[derive(Debug, Clone)]
struct CompiledJaqNativeCodec {
    format: CodecJaqFormat,
    transformations: CodecJaqTransformations,
}

#[derive(Debug, Clone)]
pub struct ProtobufCodecDescriptor {
    message: MessageDescriptor,
}

#[derive(Debug, Clone)]
struct CompiledProtobufCodec {
    message: MessageDescriptor,
    transformations: CodecJaqTransformations,
}

impl ProtobufCodecDescriptor {
    pub fn from_file_descriptor_set(
        codec: &CreateCodec,
        file_descriptor_set: prost_types::FileDescriptorSet,
        message_name: &str,
    ) -> Result<Self, CodecError> {
        let pool =
            DescriptorPool::from_file_descriptor_set(file_descriptor_set).map_err(|source| {
                CodecError::InvalidCodec {
                    codec: codec.name.as_str().to_string(),
                    reason: format!("invalid protobuf descriptor set: {source}"),
                }
            })?;
        let message =
            pool.get_message_by_name(message_name)
                .ok_or_else(|| CodecError::InvalidCodec {
                    codec: codec.name.as_str().to_string(),
                    reason: format!("protobuf message '{message_name}' was not found"),
                })?;
        Ok(Self { message })
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeRecord {
    fields: HashMap<String, RuntimeValue>,
    metadata: RuntimeRecordMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecodedRecord {
    fields: HashMap<String, RuntimeValue>,
}

pub trait RuntimeRecordValues {
    fn value(&self, name: &str) -> Option<&RuntimeValue>;
}

#[derive(Debug, Clone)]
pub struct RuntimeRecordBatch {
    schema: Arc<ArrowSchema>,
    batch: RecordBatch,
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
    #[error("failed to parse json payload for codec '{codec}': {source}")]
    SimdJsonDecode {
        codec: String,
        #[source]
        source: simd_json::Error,
    },
    #[error("failed to parse avro payload for codec '{codec}': {source}")]
    AvroDecode {
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
    #[error("codec '{codec}' expected object payload")]
    ExpectedObject { codec: String },
    #[error("codec '{codec}' has invalid jaq transformation: {reason}")]
    InvalidJaqTransformation { codec: String, reason: String },
    #[error("codec '{codec}' jaq transformation failed: {reason}")]
    JaqTransform { codec: String, reason: String },
    #[error("codec '{codec}' missing field '{field}'")]
    MissingField { codec: String, field: String },
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

    pub fn arrow_schema(&self) -> Arc<ArrowSchema> {
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

    pub fn arrow_batch_from_records(
        &self,
        records: &[RuntimeRecord],
    ) -> Result<RuntimeRecordBatch, String> {
        let columns = self
            .fields
            .iter()
            .map(|field| self.build_arrow_column(field, records))
            .collect::<Result<Vec<_>, _>>()?;
        let batch = if columns.is_empty() {
            RecordBatch::try_new_with_options(
                self.arrow_schema.clone(),
                columns,
                &RecordBatchOptions::new().with_row_count(Some(records.len())),
            )
        } else {
            RecordBatch::try_new(self.arrow_schema.clone(), columns)
        }
        .map_err(|error| error.to_string())?;
        Ok(RuntimeRecordBatch {
            schema: self.arrow_schema.clone(),
            batch,
        })
    }

    pub(crate) fn arrow_eq_predicate(
        &self,
        batch: &RuntimeRecordBatch,
        field_name: &str,
        value: &RuntimeValue,
    ) -> Result<BooleanArray, String> {
        if batch.schema.as_ref() != self.arrow_schema.as_ref() {
            return Err(
                "arrow equality predicate schema does not match compiled schema".to_string(),
            );
        }
        let column_index = self
            .arrow_schema
            .index_of(field_name)
            .map_err(|error| error.to_string())?;
        let column = batch.batch.column(column_index).as_ref();
        let Some(field) = self.fields.iter().find(|field| field.name == field_name) else {
            return Err(format!("field '{field_name}' is not declared in schema"));
        };
        let context = format!("field '{field_name}' equality predicate");

        match (&field.ty, value) {
            (ParseAsType::U8, RuntimeValue::U8(value)) => arrow_eq_scalar(
                column,
                Scalar::new(UInt8Array::from(vec![Some(*value)])),
                &context,
            ),
            (ParseAsType::I8, RuntimeValue::I8(value)) => arrow_eq_scalar(
                column,
                Scalar::new(Int8Array::from(vec![Some(*value)])),
                &context,
            ),
            (ParseAsType::U16, RuntimeValue::U16(value)) => arrow_eq_scalar(
                column,
                Scalar::new(UInt16Array::from(vec![Some(*value)])),
                &context,
            ),
            (ParseAsType::I16, RuntimeValue::I16(value)) => arrow_eq_scalar(
                column,
                Scalar::new(Int16Array::from(vec![Some(*value)])),
                &context,
            ),
            (ParseAsType::U32, RuntimeValue::U32(value)) => arrow_eq_scalar(
                column,
                Scalar::new(UInt32Array::from(vec![Some(*value)])),
                &context,
            ),
            (ParseAsType::I32, RuntimeValue::I32(value)) => arrow_eq_scalar(
                column,
                Scalar::new(Int32Array::from(vec![Some(*value)])),
                &context,
            ),
            (ParseAsType::U64, RuntimeValue::U64(value)) => arrow_eq_scalar(
                column,
                Scalar::new(UInt64Array::from(vec![Some(*value)])),
                &context,
            ),
            (ParseAsType::I64, RuntimeValue::I64(value)) => arrow_eq_scalar(
                column,
                Scalar::new(Int64Array::from(vec![Some(*value)])),
                &context,
            ),
            (ParseAsType::Bool, RuntimeValue::Bool(value)) => arrow_eq_scalar(
                column,
                Scalar::new(BooleanArray::from(vec![Some(*value)])),
                &context,
            ),
            (ParseAsType::String, RuntimeValue::String(value)) => arrow_eq_scalar(
                column,
                Scalar::new(StringArray::from(vec![Some(value.as_str())])),
                &context,
            ),
            (ParseAsType::Datetime, RuntimeValue::Datetime(value)) => {
                let Some(value) = value.timestamp_nanos_opt() else {
                    return Err(format!(
                        "field '{field_name}' datetime is out of nanosecond range"
                    ));
                };
                arrow_eq_scalar(
                    column,
                    Scalar::new(
                        TimestampNanosecondArray::from(vec![Some(value)]).with_timezone_utc(),
                    ),
                    &context,
                )
            }
            (ParseAsType::F32, RuntimeValue::F32(value)) => arrow_eq_scalar(
                column,
                Scalar::new(Float32Array::from(vec![Some(value.into_inner())])),
                &context,
            ),
            (ParseAsType::F64, RuntimeValue::F64(value)) => arrow_eq_scalar(
                column,
                Scalar::new(Float64Array::from(vec![Some(value.into_inner())])),
                &context,
            ),
            (ParseAsType::Array { .. } | ParseAsType::Vec { .. }, _) => Err(format!(
                "field '{field_name}' cannot be used for Arrow parametrization equality because \
                 list-valued branch fields are not supported"
            )),
            (expected, actual) => Err(format!(
                "field '{field_name}' has declared type {expected:?}, but branch value is \
                 {actual:?}"
            )),
        }
    }

    pub fn decoded_records_from_arrow_batch(
        &self,
        batch: &RuntimeRecordBatch,
    ) -> Result<Vec<DecodedRecord>, String> {
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

        let mut records = Vec::with_capacity(batch.batch.num_rows());
        for row_index in 0..batch.batch.num_rows() {
            let mut fields = HashMap::with_capacity(self.fields.len());
            for (column_index, field) in self.fields.iter().enumerate() {
                let value = runtime_value_from_arrow_array(
                    batch.batch.column(column_index).as_ref(),
                    &field.ty,
                    field.optional,
                    row_index,
                    &field.name,
                )?;
                if let Some(value) = value {
                    fields.insert(field.name.clone(), value);
                }
            }
            records.push(DecodedRecord { fields });
        }

        Ok(records)
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

    fn build_arrow_column(
        &self,
        field: &CompiledSchemaField,
        records: &[RuntimeRecord],
    ) -> Result<ArrayRef, String> {
        match &field.ty {
            ParseAsType::U8 => Ok(Arc::new(UInt8Array::from(collect_optional_typed_values(
                records,
                field,
                RuntimeValue::as_u8,
            )?))),
            ParseAsType::I8 => Ok(Arc::new(Int8Array::from(collect_optional_typed_values(
                records,
                field,
                RuntimeValue::as_i8,
            )?))),
            ParseAsType::U16 => Ok(Arc::new(UInt16Array::from(collect_optional_typed_values(
                records,
                field,
                RuntimeValue::as_u16,
            )?))),
            ParseAsType::I16 => Ok(Arc::new(Int16Array::from(collect_optional_typed_values(
                records,
                field,
                RuntimeValue::as_i16,
            )?))),
            ParseAsType::U32 => Ok(Arc::new(UInt32Array::from(collect_optional_typed_values(
                records,
                field,
                RuntimeValue::as_u32,
            )?))),
            ParseAsType::I32 => Ok(Arc::new(Int32Array::from(collect_optional_typed_values(
                records,
                field,
                RuntimeValue::as_i32,
            )?))),
            ParseAsType::U64 => Ok(Arc::new(UInt64Array::from(collect_optional_typed_values(
                records,
                field,
                RuntimeValue::as_u64,
            )?))),
            ParseAsType::I64 => Ok(Arc::new(Int64Array::from(collect_optional_typed_values(
                records,
                field,
                RuntimeValue::as_i64,
            )?))),
            ParseAsType::Bool => Ok(Arc::new(BooleanArray::from(collect_optional_typed_values(
                records,
                field,
                RuntimeValue::as_bool,
            )?))),
            ParseAsType::String => Ok(Arc::new(StringArray::from(collect_optional_typed_values(
                records,
                field,
                |value| value.as_string().map(str::to_owned),
            )?))),
            ParseAsType::Datetime => Ok(Arc::new(
                TimestampNanosecondArray::from(collect_optional_typed_values(
                    records,
                    field,
                    |value| {
                        value
                            .as_datetime()
                            .and_then(|value| value.timestamp_nanos_opt())
                    },
                )?)
                .with_timezone_utc(),
            )),
            ParseAsType::F32 => Ok(Arc::new(Float32Array::from(collect_optional_typed_values(
                records,
                field,
                RuntimeValue::as_f32,
            )?))),
            ParseAsType::F64 => Ok(Arc::new(Float64Array::from(collect_optional_typed_values(
                records,
                field,
                RuntimeValue::as_f64,
            )?))),
            ParseAsType::Array { element, len } => {
                build_fixed_size_list_column(records, field, element, *len)
            }
            ParseAsType::Vec { element } => build_list_column(records, field, element),
        }
    }
}

fn arrow_eq_scalar<A: Array>(
    column: &dyn Array,
    scalar: Scalar<A>,
    context: &str,
) -> Result<BooleanArray, String> {
    let left = &column as &dyn Datum;
    let right = &scalar as &dyn Datum;
    arrow_eq(left, right).map_err(|error| format!("{context} failed: {error}"))
}

impl CompiledCodec {
    pub(crate) fn schema(&self) -> Arc<CompiledSchema> {
        self.schema.clone()
    }
}

impl CompiledCodec {
    pub fn requires_blocking_decode(&self) -> bool {
        match &self.wire_schema {
            CompiledWireSchema::JaqNative(native) => native.transformations.on_ingestion.is_some(),
            CompiledWireSchema::Protobuf(protobuf) => {
                protobuf.transformations.on_ingestion.is_some()
            }
            CompiledWireSchema::Json(_) | CompiledWireSchema::Avro(_) => false,
        }
    }

    pub fn requires_blocking_encode(&self) -> bool {
        match &self.wire_schema {
            CompiledWireSchema::JaqNative(native) => native.transformations.on_emitting.is_some(),
            CompiledWireSchema::Protobuf(protobuf) => {
                protobuf.transformations.on_emitting.is_some()
            }
            CompiledWireSchema::Json(_) | CompiledWireSchema::Avro(_) => false,
        }
    }
}

impl RuntimeRecord {
    pub(crate) fn from_fields_with_metadata(
        fields: impl IntoIterator<Item = (String, RuntimeValue)>,
        metadata: RuntimeRecordMetadata,
    ) -> Self {
        Self {
            fields: fields.into_iter().collect(),
            metadata,
        }
    }

    #[cfg(test)]
    pub(crate) fn from_fields(fields: impl IntoIterator<Item = (String, RuntimeValue)>) -> Self {
        Self::from_fields_with_metadata(fields, RuntimeRecordMetadata::test())
    }

    pub fn to_json_string(&self) -> String {
        let mut keys = self.fields.keys().cloned().collect::<Vec<_>>();
        keys.sort();

        let mut json = JsonMap::new();
        for key in keys {
            if let Some(value) = self.fields.get(&key) {
                json.insert(key, value.to_json_value());
            }
        }

        JsonValue::Object(json).to_string()
    }

    pub fn to_json_string_masking(&self, sensitivity: &nervix_vm::SchemaSensitivity) -> String {
        let mut keys = self.fields.keys().cloned().collect::<Vec<_>>();
        keys.sort();

        let mut json = JsonMap::new();
        for key in keys {
            if let Some(value) = self.fields.get(&key) {
                let json_value = if sensitivity.is_sensitive(&key) {
                    JsonValue::String("<masked>".to_string())
                } else {
                    value.to_json_value()
                };
                json.insert(key, json_value);
            }
        }

        JsonValue::Object(json).to_string()
    }

    pub fn value(&self, name: &str) -> Option<&RuntimeValue> {
        self.fields.get(name)
    }

    pub(crate) fn fields(&self) -> impl Iterator<Item = (&str, &RuntimeValue)> {
        self.fields
            .iter()
            .map(|(name, value)| (name.as_str(), value))
    }

    pub(crate) fn estimated_bytes(&self) -> u64 {
        self.fields()
            .map(|(name, value)| {
                u64::try_from(name.len())
                    .unwrap_or(u64::MAX)
                    .saturating_add(value.estimated_bytes())
            })
            .sum::<u64>()
            .saturating_add(32)
    }

    pub fn metadata(&self) -> &RuntimeRecordMetadata {
        &self.metadata
    }

    pub fn with_metadata(mut self, metadata: RuntimeRecordMetadata) -> Self {
        self.metadata = metadata;
        self
    }

    pub fn with_ingested_at_watermarks(mut self, watermark: Timestamp) -> Self {
        self.metadata.ingested_at_low_watermark = watermark;
        self.metadata.ingested_at_high_watermark = watermark;
        self
    }

    pub fn to_remote(&self) -> RemoteRuntimeRecord {
        let mut names = self.fields.keys().cloned().collect::<Vec<_>>();
        names.sort();
        let fields = names
            .into_iter()
            .filter_map(|name| {
                self.fields.get(&name).map(|value| RemoteRuntimeField {
                    name,
                    value: value.to_remote(),
                })
            })
            .collect();
        RemoteRuntimeRecord {
            fields,
            metadata: self.metadata.to_remote(),
        }
    }

    pub fn from_remote(record: RemoteRuntimeRecord) -> Self {
        let fields = record
            .fields
            .into_iter()
            .map(|field| (field.name, RuntimeValue::from_remote(field.value)))
            .collect();
        Self {
            fields,
            metadata: RuntimeRecordMetadata::from_remote(record.metadata),
        }
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

impl RuntimeRecordValues for RuntimeRecord {
    fn value(&self, name: &str) -> Option<&RuntimeValue> {
        self.fields.get(name)
    }
}

impl DecodedRecord {
    pub(crate) fn from_fields(fields: impl IntoIterator<Item = (String, RuntimeValue)>) -> Self {
        Self {
            fields: fields.into_iter().collect(),
        }
    }

    pub fn to_json_string(&self) -> String {
        let mut keys = self.fields.keys().cloned().collect::<Vec<_>>();
        keys.sort();

        let mut json = JsonMap::new();
        for key in keys {
            if let Some(value) = self.fields.get(&key) {
                json.insert(key, value.to_json_value());
            }
        }

        JsonValue::Object(json).to_string()
    }

    pub fn value(&self, name: &str) -> Option<&RuntimeValue> {
        self.fields.get(name)
    }

    pub fn into_runtime_record(self, metadata: RuntimeRecordMetadata) -> RuntimeRecord {
        RuntimeRecord::from_fields_with_metadata(self.fields, metadata)
    }

    pub fn to_remote(&self) -> RemoteDecodedRecord {
        let mut names = self.fields.keys().cloned().collect::<Vec<_>>();
        names.sort();
        let fields = names
            .into_iter()
            .filter_map(|name| {
                self.fields.get(&name).map(|value| RemoteRuntimeField {
                    name,
                    value: value.to_remote(),
                })
            })
            .collect();
        RemoteDecodedRecord { fields }
    }

    pub fn from_remote(record: RemoteDecodedRecord) -> Self {
        Self {
            fields: record
                .fields
                .into_iter()
                .map(|field| (field.name, RuntimeValue::from_remote(field.value)))
                .collect(),
        }
    }
}

impl RuntimeRecordValues for DecodedRecord {
    fn value(&self, name: &str) -> Option<&RuntimeValue> {
        self.fields.get(name)
    }
}

impl RuntimeRecordBatch {
    pub(crate) fn from_record_batch(
        expected_schema: Arc<ArrowSchema>,
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

    pub fn schema(&self) -> Arc<ArrowSchema> {
        self.schema.clone()
    }

    pub fn batch(&self) -> &RecordBatch {
        &self.batch
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

    pub fn from_arrow_ipc_bytes(
        expected_schema: Arc<ArrowSchema>,
        bytes: &[u8],
    ) -> Result<Self, String> {
        let reader =
            StreamReader::try_new(Cursor::new(bytes), None).map_err(|error| error.to_string())?;
        if reader.schema().as_ref() != expected_schema.as_ref() {
            return Err("arrow IPC schema does not match expected schema".to_string());
        }
        let batches = reader
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| error.to_string())?;
        if batches.is_empty() {
            let batch = RecordBatch::try_new_with_options(
                expected_schema.clone(),
                Vec::new(),
                &RecordBatchOptions::new().with_row_count(Some(0)),
            )
            .map_err(|error| error.to_string())?;
            return Ok(Self {
                schema: expected_schema,
                batch,
            });
        }
        let runtime_batches = batches
            .into_iter()
            .map(|batch| Self {
                schema: expected_schema.clone(),
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
            Self::Array(v) => RemoteRuntimeValue::Array(
                v.iter()
                    .map(RuntimeValue::to_remote_element)
                    .collect::<Result<Vec<_>, _>>()
                    .expect("runtime arrays must contain scalar values"),
            ),
            Self::Vec(v) => RemoteRuntimeValue::Vec(
                v.iter()
                    .map(RuntimeValue::to_remote_element)
                    .collect::<Result<Vec<_>, _>>()
                    .expect("runtime vectors must contain scalar values"),
            ),
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

    fn to_remote_element(&self) -> Result<RemoteRuntimeElementValue, ()> {
        match self {
            Self::U8(v) => Ok(RemoteRuntimeElementValue::U8(*v)),
            Self::I8(v) => Ok(RemoteRuntimeElementValue::I8(*v)),
            Self::U16(v) => Ok(RemoteRuntimeElementValue::U16(*v)),
            Self::I16(v) => Ok(RemoteRuntimeElementValue::I16(*v)),
            Self::U32(v) => Ok(RemoteRuntimeElementValue::U32(*v)),
            Self::I32(v) => Ok(RemoteRuntimeElementValue::I32(*v)),
            Self::U64(v) => Ok(RemoteRuntimeElementValue::U64(*v)),
            Self::I64(v) => Ok(RemoteRuntimeElementValue::I64(*v)),
            Self::Bool(v) => Ok(RemoteRuntimeElementValue::Bool(*v)),
            Self::String(v) => Ok(RemoteRuntimeElementValue::String(v.clone())),
            Self::Datetime(v) => Ok(RemoteRuntimeElementValue::Datetime(v.to_rfc3339())),
            Self::F32(v) => Ok(RemoteRuntimeElementValue::F32(v.into_inner())),
            Self::F64(v) => Ok(RemoteRuntimeElementValue::F64(v.into_inner())),
            Self::Array(_) | Self::Vec(_) => Err(()),
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

    fn as_u8(&self) -> Option<u8> {
        if let Self::U8(value) = self {
            Some(*value)
        } else {
            None
        }
    }

    fn as_i8(&self) -> Option<i8> {
        if let Self::I8(value) = self {
            Some(*value)
        } else {
            None
        }
    }

    fn as_u16(&self) -> Option<u16> {
        if let Self::U16(value) = self {
            Some(*value)
        } else {
            None
        }
    }

    fn as_i16(&self) -> Option<i16> {
        if let Self::I16(value) = self {
            Some(*value)
        } else {
            None
        }
    }

    fn as_u32(&self) -> Option<u32> {
        if let Self::U32(value) = self {
            Some(*value)
        } else {
            None
        }
    }

    fn as_i32(&self) -> Option<i32> {
        if let Self::I32(value) = self {
            Some(*value)
        } else {
            None
        }
    }

    fn as_u64(&self) -> Option<u64> {
        if let Self::U64(value) = self {
            Some(*value)
        } else {
            None
        }
    }

    fn as_i64(&self) -> Option<i64> {
        if let Self::I64(value) = self {
            Some(*value)
        } else {
            None
        }
    }

    fn as_bool(&self) -> Option<bool> {
        if let Self::Bool(value) = self {
            Some(*value)
        } else {
            None
        }
    }

    fn as_string(&self) -> Option<&str> {
        if let Self::String(value) = self {
            Some(value.as_str())
        } else {
            None
        }
    }

    fn as_datetime(&self) -> Option<&DateTime<FixedOffset>> {
        if let Self::Datetime(value) = self {
            Some(value)
        } else {
            None
        }
    }

    fn as_f32(&self) -> Option<f32> {
        if let Self::F32(value) = self {
            Some(value.into_inner())
        } else {
            None
        }
    }

    fn as_f64(&self) -> Option<f64> {
        if let Self::F64(value) = self {
            Some(value.into_inner())
        } else {
            None
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
        arrow_schema: Arc::new(ArrowSchema::new(arrow_fields)),
    }
}

pub fn compile_codec(
    codec: &CreateCodec,
    schema: Arc<CompiledSchema>,
    wire_schema: Option<&CreateWireSchemaStmt>,
) -> Result<Arc<CompiledCodec>, CodecError> {
    compile_codec_with_protobuf(codec, schema, wire_schema, None)
}

pub fn compile_codec_with_protobuf(
    codec: &CreateCodec,
    schema: Arc<CompiledSchema>,
    wire_schema: Option<&CreateWireSchemaStmt>,
    protobuf_descriptor: Option<ProtobufCodecDescriptor>,
) -> Result<Arc<CompiledCodec>, CodecError> {
    let wire_schema = match (&codec.wire_format, wire_schema) {
        (CodecWireFormat::Json, Some(CreateWireSchemaStmt::Json(schema_def))) => {
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
            CompiledWireSchema::Json(CompiledJsonWireSchema { fields })
        }
        (CodecWireFormat::Avro, Some(CreateWireSchemaStmt::Avro(schema_def))) => {
            let schema_json = avro_schema_json(schema_def, schema.fields());
            let parsed =
                AvroSchema::parse_str(&schema_json).map_err(|source| CodecError::InvalidCodec {
                    codec: codec.name.as_str().to_string(),
                    reason: source.to_string(),
                })?;
            let fields = schema_def
                .fields
                .iter()
                .map(|field| {
                    (
                        field.name.as_str().to_string(),
                        CompiledAvroWireField {
                            ty: field.ty,
                            optional: field.optional,
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
            if let Some(program) = transformations.on_ingestion.as_deref() {
                validate_jaq_program(codec, program)?;
            }
            if let Some(program) = transformations.on_emitting.as_deref() {
                validate_jaq_program(codec, program)?;
            }
            CompiledWireSchema::JaqNative(CompiledJaqNativeCodec {
                format: *format,
                transformations: transformations.clone(),
            })
        }
        (CodecWireFormat::Protobuf(config), None) => {
            if !config.transformations.has_any() {
                return Err(CodecError::InvalidCodec {
                    codec: codec.name.as_str().to_string(),
                    reason: "protobuf codec must declare a JAQ transformation".to_string(),
                });
            }
            if let Some(program) = config.transformations.on_ingestion.as_deref() {
                validate_jaq_program(codec, program)?;
            }
            if let Some(program) = config.transformations.on_emitting.as_deref() {
                validate_jaq_program(codec, program)?;
            }
            let descriptor = protobuf_descriptor.ok_or_else(|| CodecError::InvalidCodec {
                codec: codec.name.as_str().to_string(),
                reason: "protobuf codec is missing compiled descriptor".to_string(),
            })?;
            CompiledWireSchema::Protobuf(CompiledProtobufCodec {
                message: descriptor.message,
                transformations: config.transformations.clone(),
            })
        }
        (CodecWireFormat::Json, Some(CreateWireSchemaStmt::Avro(_))) => {
            return Err(CodecError::InvalidCodec {
                codec: codec.name.as_str().to_string(),
                reason: "codec declares JSON wire format but references an avro wire schema"
                    .to_string(),
            });
        }
        (CodecWireFormat::Avro, Some(CreateWireSchemaStmt::Json(_))) => {
            return Err(CodecError::InvalidCodec {
                codec: codec.name.as_str().to_string(),
                reason: "codec declares AVRO wire format but references a json wire schema"
                    .to_string(),
            });
        }
        (CodecWireFormat::Json, None) => {
            return Err(CodecError::InvalidCodec {
                codec: codec.name.as_str().to_string(),
                reason: "codec declares JSON wire format but has no wire schema".to_string(),
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
    };

    Ok(Arc::new(CompiledCodec {
        name: codec.name.clone(),
        schema,
        wire_schema,
    }))
}

pub fn decode_with_codec(
    codec: &CompiledCodec,
    payload: &[u8],
) -> Result<DecodedRecord, CodecError> {
    match &codec.wire_schema {
        CompiledWireSchema::Json(wire_schema) => decode_json(codec, wire_schema, payload),
        CompiledWireSchema::Avro(wire_schema) => decode_avro(codec, wire_schema, payload),
        CompiledWireSchema::JaqNative(native) => decode_jaq_native(codec, native, payload),
        CompiledWireSchema::Protobuf(protobuf) => decode_protobuf(codec, protobuf, payload),
    }
}

pub(crate) fn decode_with_codec_owned(
    codec: &CompiledCodec,
    mut payload: Vec<u8>,
) -> Result<DecodedRecord, CodecError> {
    match &codec.wire_schema {
        CompiledWireSchema::Json(wire_schema) => decode_json_mut(codec, wire_schema, &mut payload),
        CompiledWireSchema::Avro(wire_schema) => decode_avro(codec, wire_schema, &payload),
        CompiledWireSchema::JaqNative(native) => decode_jaq_native(codec, native, &payload),
        CompiledWireSchema::Protobuf(protobuf) => decode_protobuf(codec, protobuf, &payload),
    }
}

pub fn encode_with_codec(
    codec: &CompiledCodec,
    record: &RuntimeRecord,
) -> Result<Vec<u8>, CodecError> {
    match &codec.wire_schema {
        CompiledWireSchema::Json(_) => encode_json(codec, record),
        CompiledWireSchema::Avro(wire_schema) => encode_avro(codec, wire_schema, record),
        CompiledWireSchema::JaqNative(native) => encode_jaq_native(codec, native, record),
        CompiledWireSchema::Protobuf(protobuf) => encode_protobuf(codec, protobuf, record),
    }
}

fn decode_json(
    codec: &CompiledCodec,
    wire_schema: &CompiledJsonWireSchema,
    payload: &[u8],
) -> Result<DecodedRecord, CodecError> {
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
) -> Result<DecodedRecord, CodecError> {
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
) -> Result<DecodedRecord, CodecError> {
    decode_json_value(codec, &value, Some(&wire_schema.fields))
}

fn decode_jaq_native(
    codec: &CompiledCodec,
    native: &CompiledJaqNativeCodec,
    payload: &[u8],
) -> Result<DecodedRecord, CodecError> {
    let Some(program) = native.transformations.on_ingestion.as_deref() else {
        return Err(CodecError::InvalidCodec {
            codec: codec.name.as_str().to_string(),
            reason: "JAQ-native codec used for decoding must declare ON INGESTION transformation"
                .to_string(),
        });
    };
    let value = parse_jaq_native_payload(codec, native.format, payload)?;
    let value = run_jaq_transformation(codec, program, value)?;
    decode_json_value(codec, &value, None)
}

fn decode_protobuf(
    codec: &CompiledCodec,
    protobuf: &CompiledProtobufCodec,
    payload: &[u8],
) -> Result<DecodedRecord, CodecError> {
    let Some(program) = protobuf.transformations.on_ingestion.as_deref() else {
        return Err(CodecError::InvalidCodec {
            codec: codec.name.as_str().to_string(),
            reason: "protobuf codec used for decoding must declare ON INGESTION transformation"
                .to_string(),
        });
    };
    let message = DynamicMessage::decode(protobuf.message.clone(), payload).map_err(|source| {
        CodecError::ProtobufDecode {
            codec: codec.name.as_str().to_string(),
            reason: source.to_string(),
        }
    })?;
    let value = protobuf_message_to_json(codec, &message)?;
    let value = run_jaq_transformation(codec, program, value)?;
    decode_json_value(codec, &value, None)
}

fn protobuf_message_to_json(
    codec: &CompiledCodec,
    message: &DynamicMessage,
) -> Result<JsonValue, CodecError> {
    let mut encoded = Vec::new();
    let mut serializer = serde_json::Serializer::new(&mut encoded);
    let options = ProtobufSerializeOptions::new()
        .use_proto_field_name(true)
        .stringify_64_bit_integers(false);
    message
        .serialize_with_options(&mut serializer, &options)
        .map_err(|source| CodecError::ProtobufDecode {
            codec: codec.name.as_str().to_string(),
            reason: source.to_string(),
        })?;
    serde_json::from_slice(&encoded).map_err(|source| CodecError::JsonDecode {
        codec: codec.name.as_str().to_string(),
        source,
    })
}

fn parse_jaq_native_payload(
    codec: &CompiledCodec,
    format: CodecJaqFormat,
    payload: &[u8],
) -> Result<JsonValue, CodecError> {
    let bytes = Bytes::copy_from_slice(payload);
    let jaq_format = codec_jaq_format_to_jaq(format);
    let format_name = codec_jaq_format_name(format);
    let source =
        jaq_read::bytes_str(jaq_format, &bytes).map_err(|error| CodecError::JaqNativeDecode {
            codec: codec.name.as_str().to_string(),
            format: format_name,
            reason: error.to_string(),
        })?;
    let mut values = jaq_read::parse(jaq_format, &bytes, source, false);
    let value = values
        .next()
        .ok_or_else(|| CodecError::JaqNativeDecode {
            codec: codec.name.as_str().to_string(),
            format: format_name,
            reason: "payload produced no input values".to_string(),
        })?
        .map_err(|error| CodecError::JaqNativeDecode {
            codec: codec.name.as_str().to_string(),
            format: format_name,
            reason: error.to_string(),
        })?;
    if values.next().is_some() {
        return Err(CodecError::JaqNativeDecode {
            codec: codec.name.as_str().to_string(),
            format: format_name,
            reason: "payload produced multiple input values".to_string(),
        });
    }
    jaq_value_to_json(value).map_err(|reason| CodecError::JaqNativeDecode {
        codec: codec.name.as_str().to_string(),
        format: format_name,
        reason,
    })
}

fn write_jaq_native_payload(
    codec: &CompiledCodec,
    format: CodecJaqFormat,
    value: JsonValue,
) -> Result<Vec<u8>, CodecError> {
    let format_name = codec_jaq_format_name(format);
    let value: JaqVal =
        serde_json::from_value(value).map_err(|error| CodecError::JaqNativeEncode {
            codec: codec.name.as_str().to_string(),
            format: format_name,
            reason: error.to_string(),
        })?;
    let mut encoded = Vec::new();
    let writer = JaqWriter {
        format: codec_jaq_format_to_jaq(format),
        pp: jaq_json::write::Pp {
            sep_space: true,
            ..Default::default()
        },
        join: true,
    };
    jaq_write::write(&mut encoded, &writer, &value).map_err(|error| {
        CodecError::JaqNativeEncode {
            codec: codec.name.as_str().to_string(),
            format: format_name,
            reason: error.to_string(),
        }
    })?;
    Ok(encoded)
}

fn codec_jaq_format_to_jaq(format: CodecJaqFormat) -> JaqFormat {
    match format {
        CodecJaqFormat::Json => JaqFormat::Json,
        CodecJaqFormat::Yaml => JaqFormat::Yaml,
        CodecJaqFormat::Toml => JaqFormat::Toml,
        CodecJaqFormat::Xml => JaqFormat::Xml,
        CodecJaqFormat::Cbor => JaqFormat::Cbor,
    }
}

fn codec_jaq_format_name(format: CodecJaqFormat) -> &'static str {
    match format {
        CodecJaqFormat::Json => "JSON",
        CodecJaqFormat::Yaml => "YAML",
        CodecJaqFormat::Toml => "TOML",
        CodecJaqFormat::Xml => "XML",
        CodecJaqFormat::Cbor => "CBOR",
    }
}

fn decode_json_value(
    codec: &CompiledCodec,
    value: &JsonValue,
    wire_fields: Option<&HashMap<String, CompiledJsonWireField>>,
) -> Result<DecodedRecord, CodecError> {
    let JsonValue::Object(object) = value else {
        return Err(CodecError::ExpectedObject {
            codec: codec.name.as_str().to_string(),
        });
    };

    let mut fields = HashMap::new();
    for field in codec.schema.fields() {
        let wire_field = wire_fields.and_then(|wire_fields| wire_fields.get(&field.name).copied());
        if wire_fields.is_some() && wire_field.is_none() {
            return Err(CodecError::InvalidCodec {
                codec: codec.name.as_str().to_string(),
                reason: format!("missing wire field '{}'", field.name),
            });
        }
        let Some(value) = object.get(&field.name) else {
            if field.optional && wire_field.is_none_or(|wire_field| wire_field.optional) {
                continue;
            }
            return Err(CodecError::MissingField {
                codec: codec.name.as_str().to_string(),
                field: field.name.clone(),
            });
        };
        if value.is_null() {
            if field.optional && wire_field.is_none_or(|wire_field| wire_field.optional) {
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
        let parsed = parse_json_value(codec, &field.name, &field.ty, value)?;
        fields.insert(field.name.clone(), parsed);
    }

    Ok(DecodedRecord::from_fields(fields))
}

fn validate_jaq_program(codec: &CreateCodec, program: &str) -> Result<(), CodecError> {
    compile_jaq_filter(program)
        .map(|_| ())
        .map_err(|reason| CodecError::InvalidJaqTransformation {
            codec: codec.name.as_str().to_string(),
            reason,
        })
}

fn run_jaq_transformation(
    codec: &CompiledCodec,
    program: &str,
    input: JsonValue,
) -> Result<JsonValue, CodecError> {
    let filter = compile_jaq_filter(program).map_err(|reason| CodecError::JaqTransform {
        codec: codec.name.as_str().to_string(),
        reason,
    })?;
    let input: JaqVal =
        serde_json::from_value(input).map_err(|reason| CodecError::JaqTransform {
            codec: codec.name.as_str().to_string(),
            reason: reason.to_string(),
        })?;
    let ctx = JaqCtx::<data::JustLut<JaqVal>>::new(&filter.lut, JaqVars::new([]));
    let mut outputs = filter.id.run((ctx, input)).map(unwrap_valr);
    let output = outputs
        .next()
        .ok_or_else(|| CodecError::JaqTransform {
            codec: codec.name.as_str().to_string(),
            reason: "transformation produced no output".to_string(),
        })?
        .map_err(|error| CodecError::JaqTransform {
            codec: codec.name.as_str().to_string(),
            reason: error.to_string(),
        })?;
    if outputs.next().is_some() {
        return Err(CodecError::JaqTransform {
            codec: codec.name.as_str().to_string(),
            reason: "transformation produced multiple outputs".to_string(),
        });
    }
    jaq_value_to_json(output).map_err(|reason| CodecError::JaqTransform {
        codec: codec.name.as_str().to_string(),
        reason,
    })
}

fn compile_jaq_filter(program: &str) -> Result<jaq_core::Filter<data::JustLut<JaqVal>>, String> {
    let defs = jaq_core::defs()
        .chain(jaq_std::defs())
        .chain(jaq_json::defs());
    let funs = jaq_core::funs()
        .chain(jaq_std::funs())
        .chain(jaq_json::funs())
        .chain(jaq_fmts::funs());
    let loader = Loader::new(defs);
    let arena = Arena::default();
    let modules = loader
        .load(
            &arena,
            File {
                code: program,
                path: (),
            },
        )
        .map_err(|errors| format!("{errors:?}"))?;
    JaqCompiler::default()
        .with_funs(funs)
        .compile(modules)
        .map_err(|errors| format!("{errors:?}"))
}

fn jaq_value_to_json(value: JaqVal) -> Result<JsonValue, String> {
    match value {
        JaqVal::Null => Ok(JsonValue::Null),
        JaqVal::Bool(value) => Ok(JsonValue::Bool(value)),
        JaqVal::Num(value) => jaq_num_to_json(value),
        JaqVal::BStr(_) => {
            Err("jaq output contains binary string, which is not valid JSON".to_string())
        }
        JaqVal::TStr(value) => String::from_utf8(value.to_vec())
            .map(JsonValue::String)
            .map_err(|error| error.to_string()),
        JaqVal::Arr(values) => values
            .iter()
            .cloned()
            .map(jaq_value_to_json)
            .collect::<Result<Vec<_>, _>>()
            .map(JsonValue::Array),
        JaqVal::Obj(values) => {
            let mut object = JsonMap::new();
            for (key, value) in values.iter() {
                let key = match key {
                    JaqVal::TStr(key) => {
                        String::from_utf8(key.to_vec()).map_err(|error| error.to_string())?
                    }
                    _ => {
                        return Err("jaq output contains a non-string object key, which is not \
                                    valid JSON"
                            .to_string());
                    }
                };
                object.insert(key, jaq_value_to_json(value.clone())?);
            }
            Ok(JsonValue::Object(object))
        }
    }
}

fn jaq_num_to_json(value: JaqNum) -> Result<JsonValue, String> {
    let rendered = value.to_string();
    serde_json::Number::from_str(&rendered)
        .map(JsonValue::Number)
        .map_err(|error| error.to_string())
}

fn decode_avro(
    codec: &CompiledCodec,
    wire_schema: &CompiledAvroWireSchema,
    payload: &[u8],
) -> Result<DecodedRecord, CodecError> {
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
    let values = values.into_iter().collect::<HashMap<_, _>>();

    let mut fields = HashMap::new();
    for field in codec.schema.fields() {
        let wire_field =
            wire_schema
                .fields
                .get(&field.name)
                .ok_or_else(|| CodecError::InvalidCodec {
                    codec: codec.name.as_str().to_string(),
                    reason: format!("missing wire field '{}'", field.name),
                })?;
        let Some(value) = values.get(&field.name) else {
            if field.optional && wire_field.optional {
                continue;
            }
            return Err(CodecError::MissingField {
                codec: codec.name.as_str().to_string(),
                field: field.name.clone(),
            });
        };
        if avro_value_is_null(value) {
            if field.optional && wire_field.optional {
                continue;
            }
            return Err(CodecError::ParseField {
                codec: codec.name.as_str().to_string(),
                field: field.name.clone(),
                reason: "null is incompatible with required field".to_string(),
            });
        }
        let parsed = parse_avro_value(codec, &field.name, &field.ty, value)?;
        fields.insert(field.name.clone(), parsed);
    }

    Ok(DecodedRecord::from_fields(fields))
}

fn encode_json(codec: &CompiledCodec, record: &RuntimeRecord) -> Result<Vec<u8>, CodecError> {
    match &codec.wire_schema {
        CompiledWireSchema::Json(_) => {}
        CompiledWireSchema::Avro(_)
        | CompiledWireSchema::JaqNative(_)
        | CompiledWireSchema::Protobuf(_) => {
            unreachable!("json encoder must only be used for json")
        }
    }
    let value = record_to_json_value(codec, record)?;

    serde_json::to_vec(&value).map_err(|source| CodecError::JsonDecode {
        codec: codec.name.as_str().to_string(),
        source,
    })
}

fn encode_jaq_native(
    codec: &CompiledCodec,
    native: &CompiledJaqNativeCodec,
    record: &RuntimeRecord,
) -> Result<Vec<u8>, CodecError> {
    let Some(program) = native.transformations.on_emitting.as_deref() else {
        return Err(CodecError::InvalidCodec {
            codec: codec.name.as_str().to_string(),
            reason: "JAQ-native codec used for encoding must declare ON EMITTING transformation"
                .to_string(),
        });
    };
    let value = record_to_json_value(codec, record)?;
    let value = run_jaq_transformation(codec, program, value)?;
    write_jaq_native_payload(codec, native.format, value)
}

fn encode_protobuf(
    codec: &CompiledCodec,
    protobuf: &CompiledProtobufCodec,
    record: &RuntimeRecord,
) -> Result<Vec<u8>, CodecError> {
    let Some(program) = protobuf.transformations.on_emitting.as_deref() else {
        return Err(CodecError::InvalidCodec {
            codec: codec.name.as_str().to_string(),
            reason: "protobuf codec used for encoding must declare ON EMITTING transformation"
                .to_string(),
        });
    };
    let value = record_to_json_value(codec, record)?;
    let value = run_jaq_transformation(codec, program, value)?;
    let json = serde_json::to_vec(&value).map_err(|source| CodecError::JsonDecode {
        codec: codec.name.as_str().to_string(),
        source,
    })?;
    let mut deserializer = serde_json::Deserializer::from_slice(&json);
    let options = ProtobufDeserializeOptions::new().deny_unknown_fields(true);
    let message = DynamicMessage::deserialize_with_options(
        protobuf.message.clone(),
        &mut deserializer,
        &options,
    )
    .map_err(|source| CodecError::ProtobufEncode {
        codec: codec.name.as_str().to_string(),
        reason: source.to_string(),
    })?;
    deserializer
        .end()
        .map_err(|source| CodecError::ProtobufEncode {
            codec: codec.name.as_str().to_string(),
            reason: source.to_string(),
        })?;
    let mut encoded = Vec::new();
    message
        .encode(&mut encoded)
        .map_err(|source| CodecError::ProtobufEncode {
            codec: codec.name.as_str().to_string(),
            reason: source.to_string(),
        })?;
    Ok(encoded)
}

fn encode_avro(
    codec: &CompiledCodec,
    wire_schema: &CompiledAvroWireSchema,
    record: &RuntimeRecord,
) -> Result<Vec<u8>, CodecError> {
    let mut fields = Vec::new();
    for field in codec.schema.fields() {
        let wire_ty =
            wire_schema
                .fields
                .get(&field.name)
                .ok_or_else(|| CodecError::InvalidCodec {
                    codec: codec.name.as_str().to_string(),
                    reason: format!("missing wire field '{}'", field.name),
                })?;
        let Some(value) = record.value(&field.name) else {
            if wire_ty.optional {
                fields.push((
                    field.name.clone(),
                    AvroValue::Union(0, Box::new(AvroValue::Null)),
                ));
                continue;
            }
            return Err(CodecError::EncodeField {
                codec: codec.name.as_str().to_string(),
                field: field.name.clone(),
                reason: "missing field in runtime record".to_string(),
            });
        };
        fields.push((
            field.name.clone(),
            runtime_value_to_avro(codec, &field.name, wire_ty.ty, wire_ty.optional, value)?,
        ));
    }

    to_avro_datum(&wire_schema.schema, AvroValue::Record(fields)).map_err(|source| {
        CodecError::AvroDecode {
            codec: codec.name.as_str().to_string(),
            source,
        }
    })
}

fn record_to_json_value(
    codec: &CompiledCodec,
    record: &RuntimeRecord,
) -> Result<JsonValue, CodecError> {
    let mut object = JsonMap::new();
    for field in codec.schema.fields() {
        let Some(value) = record.value(&field.name) else {
            if field.optional {
                continue;
            }
            return Err(CodecError::EncodeField {
                codec: codec.name.as_str().to_string(),
                field: field.name.clone(),
                reason: "missing field in runtime record".to_string(),
            });
        };
        object.insert(field.name.clone(), value.to_json_value());
    }

    Ok(JsonValue::Object(object))
}

fn parse_json_value(
    codec: &CompiledCodec,
    field: &str,
    ty: &ParseAsType,
    value: &JsonValue,
) -> Result<RuntimeValue, CodecError> {
    let err = |reason: String| CodecError::ParseField {
        codec: codec.name.as_str().to_string(),
        field: field.to_string(),
        reason,
    };

    match ty {
        ParseAsType::U8 => value
            .as_u64()
            .and_then(|v| u8::try_from(v).ok())
            .map(RuntimeValue::U8),
        ParseAsType::I8 => value
            .as_i64()
            .and_then(|v| i8::try_from(v).ok())
            .map(RuntimeValue::I8),
        ParseAsType::U16 => value
            .as_u64()
            .and_then(|v| u16::try_from(v).ok())
            .map(RuntimeValue::U16),
        ParseAsType::I16 => value
            .as_i64()
            .and_then(|v| i16::try_from(v).ok())
            .map(RuntimeValue::I16),
        ParseAsType::U32 => value
            .as_u64()
            .and_then(|v| u32::try_from(v).ok())
            .map(RuntimeValue::U32),
        ParseAsType::I32 => value
            .as_i64()
            .and_then(|v| i32::try_from(v).ok())
            .map(RuntimeValue::I32),
        ParseAsType::U64 => value.as_u64().map(RuntimeValue::U64),
        ParseAsType::I64 => value.as_i64().map(RuntimeValue::I64),
        ParseAsType::Bool => value.as_bool().map(RuntimeValue::Bool),
        ParseAsType::String => value.as_str().map(|v| RuntimeValue::String(v.to_string())),
        ParseAsType::Datetime => value
            .as_str()
            .and_then(|v| DateTime::parse_from_rfc3339(v).ok())
            .map(RuntimeValue::Datetime),
        ParseAsType::F32 => value
            .as_f64()
            .map(|v| RuntimeValue::F32(OrderedFloat(v as f32))),
        ParseAsType::F64 => value.as_f64().map(|v| RuntimeValue::F64(OrderedFloat(v))),
        ParseAsType::Array { element, len } => value.as_array().and_then(|values| {
            if values.len() != *len as usize {
                return None;
            }
            parse_json_array_values(codec, field, element, values)
                .ok()
                .map(RuntimeValue::Array)
        }),
        ParseAsType::Vec { element } => value
            .as_array()
            .and_then(|values| parse_json_array_values(codec, field, element, values).ok())
            .map(RuntimeValue::Vec),
    }
    .ok_or_else(|| err(format!("value {value} is incompatible with {ty:?}")))
}

fn parse_json_array_values(
    codec: &CompiledCodec,
    field: &str,
    element: &ParseAsType,
    values: &[JsonValue],
) -> Result<Vec<RuntimeValue>, CodecError> {
    values
        .iter()
        .map(|value| parse_json_value(codec, field, element, value))
        .collect()
}

fn parse_avro_value(
    codec: &CompiledCodec,
    field: &str,
    ty: &ParseAsType,
    value: &AvroValue,
) -> Result<RuntimeValue, CodecError> {
    let value = avro_value_payload(value);
    let err = |reason: String| CodecError::ParseField {
        codec: codec.name.as_str().to_string(),
        field: field.to_string(),
        reason,
    };

    match ty {
        ParseAsType::U8 => avro_to_u64(value)
            .and_then(|v| u8::try_from(v).ok())
            .map(RuntimeValue::U8),
        ParseAsType::I8 => avro_to_i64(value)
            .and_then(|v| i8::try_from(v).ok())
            .map(RuntimeValue::I8),
        ParseAsType::U16 => avro_to_u64(value)
            .and_then(|v| u16::try_from(v).ok())
            .map(RuntimeValue::U16),
        ParseAsType::I16 => avro_to_i64(value)
            .and_then(|v| i16::try_from(v).ok())
            .map(RuntimeValue::I16),
        ParseAsType::U32 => avro_to_u64(value)
            .and_then(|v| u32::try_from(v).ok())
            .map(RuntimeValue::U32),
        ParseAsType::I32 => avro_to_i64(value)
            .and_then(|v| i32::try_from(v).ok())
            .map(RuntimeValue::I32),
        ParseAsType::U64 => avro_to_u64(value).map(RuntimeValue::U64),
        ParseAsType::I64 => avro_to_i64(value).map(RuntimeValue::I64),
        ParseAsType::Bool => match value {
            AvroValue::Boolean(v) => Some(RuntimeValue::Bool(*v)),
            _ => None,
        },
        ParseAsType::String => match value {
            AvroValue::String(v) => Some(RuntimeValue::String(v.clone())),
            _ => None,
        },
        ParseAsType::Datetime => match value {
            AvroValue::String(v) => DateTime::parse_from_rfc3339(v)
                .ok()
                .map(RuntimeValue::Datetime),
            _ => None,
        },
        ParseAsType::F32 => match value {
            AvroValue::Float(v) => Some(RuntimeValue::F32(OrderedFloat(*v))),
            _ => None,
        },
        ParseAsType::F64 => match value {
            AvroValue::Float(v) => Some(RuntimeValue::F64(OrderedFloat(*v as f64))),
            AvroValue::Double(v) => Some(RuntimeValue::F64(OrderedFloat(*v))),
            _ => None,
        },
        ParseAsType::Array { element, len } => match value {
            AvroValue::Array(values) if values.len() == *len as usize => {
                parse_avro_array_values(codec, field, element, values)
                    .ok()
                    .map(RuntimeValue::Array)
            }
            _ => None,
        },
        ParseAsType::Vec { element } => match value {
            AvroValue::Array(values) => parse_avro_array_values(codec, field, element, values)
                .ok()
                .map(RuntimeValue::Vec),
            _ => None,
        },
    }
    .ok_or_else(|| err(format!("value {value:?} is incompatible with {ty:?}")))
}

fn parse_avro_array_values(
    codec: &CompiledCodec,
    field: &str,
    element: &ParseAsType,
    values: &[AvroValue],
) -> Result<Vec<RuntimeValue>, CodecError> {
    values
        .iter()
        .map(|value| parse_avro_value(codec, field, element, value))
        .collect()
}

fn runtime_value_to_avro(
    codec: &CompiledCodec,
    field: &str,
    wire_ty: AvroType,
    optional: bool,
    value: &RuntimeValue,
) -> Result<AvroValue, CodecError> {
    let err = |reason: String| CodecError::EncodeField {
        codec: codec.name.as_str().to_string(),
        field: field.to_string(),
        reason,
    };

    let value = match wire_ty {
        AvroType::Boolean => match value {
            RuntimeValue::Bool(v) => Ok(AvroValue::Boolean(*v)),
            _ => Err(err("expected bool".to_string())),
        },
        AvroType::Int => match value {
            RuntimeValue::I8(v) => Ok(AvroValue::Int(*v as i32)),
            RuntimeValue::I16(v) => Ok(AvroValue::Int(*v as i32)),
            RuntimeValue::I32(v) => Ok(AvroValue::Int(*v)),
            RuntimeValue::U8(v) => Ok(AvroValue::Int(*v as i32)),
            RuntimeValue::U16(v) => Ok(AvroValue::Int(*v as i32)),
            RuntimeValue::U32(v) if i32::try_from(*v).is_ok() => Ok(AvroValue::Int(*v as i32)),
            _ => Err(err("expected int-compatible value".to_string())),
        },
        AvroType::Long => match value {
            RuntimeValue::I8(v) => Ok(AvroValue::Long(*v as i64)),
            RuntimeValue::I16(v) => Ok(AvroValue::Long(*v as i64)),
            RuntimeValue::I32(v) => Ok(AvroValue::Long(*v as i64)),
            RuntimeValue::I64(v) => Ok(AvroValue::Long(*v)),
            RuntimeValue::U8(v) => Ok(AvroValue::Long(*v as i64)),
            RuntimeValue::U16(v) => Ok(AvroValue::Long(*v as i64)),
            RuntimeValue::U32(v) => Ok(AvroValue::Long(*v as i64)),
            RuntimeValue::U64(v) if i64::try_from(*v).is_ok() => Ok(AvroValue::Long(*v as i64)),
            _ => Err(err("expected long-compatible value".to_string())),
        },
        AvroType::Float => match value {
            RuntimeValue::F32(v) => Ok(AvroValue::Float(v.into_inner())),
            _ => Err(err("expected f32".to_string())),
        },
        AvroType::Double => match value {
            RuntimeValue::F32(v) => Ok(AvroValue::Double(v.into_inner() as f64)),
            RuntimeValue::F64(v) => Ok(AvroValue::Double(v.into_inner())),
            _ => Err(err("expected float-compatible value".to_string())),
        },
        AvroType::String => match value {
            RuntimeValue::String(v) => Ok(AvroValue::String(v.clone())),
            RuntimeValue::Datetime(v) => Ok(AvroValue::String(v.to_rfc3339())),
            _ => Err(err("expected string-compatible value".to_string())),
        },
        AvroType::Array => match value {
            RuntimeValue::Array(values) | RuntimeValue::Vec(values) => values
                .iter()
                .map(|value| runtime_value_to_avro_array_item(codec, field, value))
                .collect::<Result<Vec<_>, _>>()
                .map(AvroValue::Array),
            _ => Err(err("expected list-compatible value".to_string())),
        },
        _ => Err(err(format!("unsupported avro type {wire_ty:?}"))),
    }?;
    if optional {
        Ok(AvroValue::Union(1, Box::new(value)))
    } else {
        Ok(value)
    }
}

fn runtime_value_to_avro_array_item(
    codec: &CompiledCodec,
    field: &str,
    value: &RuntimeValue,
) -> Result<AvroValue, CodecError> {
    match value {
        RuntimeValue::Bool(v) => Ok(AvroValue::Boolean(*v)),
        RuntimeValue::I8(v) => Ok(AvroValue::Long(*v as i64)),
        RuntimeValue::I16(v) => Ok(AvroValue::Long(*v as i64)),
        RuntimeValue::I32(v) => Ok(AvroValue::Long(*v as i64)),
        RuntimeValue::I64(v) => Ok(AvroValue::Long(*v)),
        RuntimeValue::U8(v) => Ok(AvroValue::Long(*v as i64)),
        RuntimeValue::U16(v) => Ok(AvroValue::Long(*v as i64)),
        RuntimeValue::U32(v) => Ok(AvroValue::Long(*v as i64)),
        RuntimeValue::U64(v) if i64::try_from(*v).is_ok() => Ok(AvroValue::Long(*v as i64)),
        RuntimeValue::F32(v) => Ok(AvroValue::Float(v.into_inner())),
        RuntimeValue::F64(v) => Ok(AvroValue::Double(v.into_inner())),
        RuntimeValue::String(v) => Ok(AvroValue::String(v.clone())),
        RuntimeValue::Datetime(v) => Ok(AvroValue::String(v.to_rfc3339())),
        RuntimeValue::Array(_) | RuntimeValue::Vec(_) | RuntimeValue::U64(_) => {
            Err(CodecError::EncodeField {
                codec: codec.name.as_str().to_string(),
                field: field.to_string(),
                reason: "unsupported array item value".to_string(),
            })
        }
    }
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

fn arrow_data_type(ty: &ParseAsType) -> ArrowDataType {
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
            ArrowFieldRef::new(ArrowField::new("item", arrow_data_type(element), true)),
            i32::try_from(*len).expect("array length must fit Arrow fixed-size list"),
        ),
        ParseAsType::Vec { element } => ArrowDataType::List(ArrowFieldRef::new(ArrowField::new(
            "item",
            arrow_data_type(element),
            true,
        ))),
    }
}

fn collect_optional_typed_values<T>(
    records: &[RuntimeRecord],
    field: &CompiledSchemaField,
    extract: impl Fn(&RuntimeValue) -> Option<T>,
) -> Result<Vec<Option<T>>, String> {
    records
        .iter()
        .enumerate()
        .map(|(row_index, record)| {
            let Some(value) = record.value(&field.name) else {
                return if field.optional {
                    Ok(None)
                } else {
                    Err(format!(
                        "record at row {row_index} is missing schema field '{}'",
                        field.name
                    ))
                };
            };
            extract(value).map(Some).ok_or_else(|| {
                format!(
                    "record at row {row_index} field '{}' is incompatible with {:?}",
                    field.name, field.ty
                )
            })
        })
        .collect()
}

fn list_values_for_field<'a>(
    record: &'a RuntimeRecord,
    field: &CompiledSchemaField,
    row_index: usize,
) -> Result<Option<&'a [RuntimeValue]>, String> {
    let Some(value) = record.value(&field.name) else {
        return if field.optional {
            Ok(None)
        } else {
            Err(format!(
                "record at row {row_index} is missing schema field '{}'",
                field.name
            ))
        };
    };
    match value {
        RuntimeValue::Array(values) | RuntimeValue::Vec(values) => Ok(Some(values)),
        other => Err(format!(
            "record at row {row_index} field '{}' expected list value, got {}",
            field.name,
            runtime_value_type_name(other)
        )),
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

macro_rules! build_list_column_for_scalar {
    ($records:expr, $field:expr, $extract:path, $builder:ty, $array:ty) => {{
        let mut builder = ListBuilder::new(<$builder>::new());
        for (row_index, record) in $records.iter().enumerate() {
            let Some(values) = list_values_for_field(record, $field, row_index)? else {
                builder.append(false);
                continue;
            };
            for value in values {
                if let Some(value) = $extract(value) {
                    builder.values().append_value(value);
                } else {
                    builder.values().append_null();
                }
            }
            builder.append(true);
        }
        Ok(Arc::new(builder.finish()) as ArrayRef)
    }};
}

macro_rules! build_fixed_size_list_column_for_scalar {
    ($records:expr, $field:expr, $len:expr, $extract:path, $builder:ty) => {{
        let list_len = i32::try_from($len)
            .map_err(|_| format!("array field '{}' length is too large", $field.name))?;
        let mut builder = FixedSizeListBuilder::new(<$builder>::new(), list_len);
        for (row_index, record) in $records.iter().enumerate() {
            let Some(values) = list_values_for_field(record, $field, row_index)? else {
                builder.append(false);
                continue;
            };
            if values.len() != $len as usize {
                return Err(format!(
                    "record at row {row_index} field '{}' expected array length {}, got {}",
                    $field.name,
                    $len,
                    values.len()
                ));
            }
            for value in values {
                if let Some(value) = $extract(value) {
                    builder.values().append_value(value);
                } else {
                    builder.values().append_null();
                }
            }
            builder.append(true);
        }
        Ok(Arc::new(builder.finish()) as ArrayRef)
    }};
}

fn build_list_column(
    records: &[RuntimeRecord],
    field: &CompiledSchemaField,
    element: &ParseAsType,
) -> Result<ArrayRef, String> {
    match element {
        ParseAsType::U8 => build_list_column_for_scalar!(
            records,
            field,
            RuntimeValue::as_u8,
            UInt8Builder,
            UInt8Array
        ),
        ParseAsType::I8 => build_list_column_for_scalar!(
            records,
            field,
            RuntimeValue::as_i8,
            Int8Builder,
            Int8Array
        ),
        ParseAsType::U16 => build_list_column_for_scalar!(
            records,
            field,
            RuntimeValue::as_u16,
            UInt16Builder,
            UInt16Array
        ),
        ParseAsType::I16 => build_list_column_for_scalar!(
            records,
            field,
            RuntimeValue::as_i16,
            Int16Builder,
            Int16Array
        ),
        ParseAsType::U32 => build_list_column_for_scalar!(
            records,
            field,
            RuntimeValue::as_u32,
            UInt32Builder,
            UInt32Array
        ),
        ParseAsType::I32 => build_list_column_for_scalar!(
            records,
            field,
            RuntimeValue::as_i32,
            Int32Builder,
            Int32Array
        ),
        ParseAsType::U64 => build_list_column_for_scalar!(
            records,
            field,
            RuntimeValue::as_u64,
            UInt64Builder,
            UInt64Array
        ),
        ParseAsType::I64 => build_list_column_for_scalar!(
            records,
            field,
            RuntimeValue::as_i64,
            Int64Builder,
            Int64Array
        ),
        ParseAsType::Bool => build_list_column_for_scalar!(
            records,
            field,
            RuntimeValue::as_bool,
            BooleanBuilder,
            BooleanArray
        ),
        ParseAsType::F32 => build_list_column_for_scalar!(
            records,
            field,
            RuntimeValue::as_f32,
            Float32Builder,
            Float32Array
        ),
        ParseAsType::F64 => build_list_column_for_scalar!(
            records,
            field,
            RuntimeValue::as_f64,
            Float64Builder,
            Float64Array
        ),
        ParseAsType::String => {
            let mut builder = ListBuilder::new(StringBuilder::new());
            for (row_index, record) in records.iter().enumerate() {
                let Some(values) = list_values_for_field(record, field, row_index)? else {
                    builder.append(false);
                    continue;
                };
                for value in values {
                    if let Some(value) = value.as_string() {
                        builder.values().append_value(value);
                    } else {
                        builder.values().append_null();
                    }
                }
                builder.append(true);
            }
            Ok(Arc::new(builder.finish()))
        }
        ParseAsType::Datetime => {
            let value_builder = TimestampNanosecondBuilder::new().with_data_type(
                ArrowDataType::Timestamp(ArrowTimeUnit::Nanosecond, Some("+00:00".into())),
            );
            let mut builder = ListBuilder::new(value_builder);
            for (row_index, record) in records.iter().enumerate() {
                let Some(values) = list_values_for_field(record, field, row_index)? else {
                    builder.append(false);
                    continue;
                };
                for value in values {
                    if let Some(value) = value
                        .as_datetime()
                        .and_then(|value| value.timestamp_nanos_opt())
                    {
                        builder.values().append_value(value);
                    } else {
                        builder.values().append_null();
                    }
                }
                builder.append(true);
            }
            Ok(Arc::new(builder.finish()))
        }
        ParseAsType::Array { .. } | ParseAsType::Vec { .. } => Err(format!(
            "field '{}' uses unsupported list element type {:?}",
            field.name, element
        )),
    }
}

fn build_fixed_size_list_column(
    records: &[RuntimeRecord],
    field: &CompiledSchemaField,
    element: &ParseAsType,
    len: u32,
) -> Result<ArrayRef, String> {
    match element {
        ParseAsType::U8 => build_fixed_size_list_column_for_scalar!(
            records,
            field,
            len,
            RuntimeValue::as_u8,
            UInt8Builder
        ),
        ParseAsType::I8 => build_fixed_size_list_column_for_scalar!(
            records,
            field,
            len,
            RuntimeValue::as_i8,
            Int8Builder
        ),
        ParseAsType::U16 => build_fixed_size_list_column_for_scalar!(
            records,
            field,
            len,
            RuntimeValue::as_u16,
            UInt16Builder
        ),
        ParseAsType::I16 => build_fixed_size_list_column_for_scalar!(
            records,
            field,
            len,
            RuntimeValue::as_i16,
            Int16Builder
        ),
        ParseAsType::U32 => build_fixed_size_list_column_for_scalar!(
            records,
            field,
            len,
            RuntimeValue::as_u32,
            UInt32Builder
        ),
        ParseAsType::I32 => build_fixed_size_list_column_for_scalar!(
            records,
            field,
            len,
            RuntimeValue::as_i32,
            Int32Builder
        ),
        ParseAsType::U64 => build_fixed_size_list_column_for_scalar!(
            records,
            field,
            len,
            RuntimeValue::as_u64,
            UInt64Builder
        ),
        ParseAsType::I64 => build_fixed_size_list_column_for_scalar!(
            records,
            field,
            len,
            RuntimeValue::as_i64,
            Int64Builder
        ),
        ParseAsType::Bool => build_fixed_size_list_column_for_scalar!(
            records,
            field,
            len,
            RuntimeValue::as_bool,
            BooleanBuilder
        ),
        ParseAsType::F32 => build_fixed_size_list_column_for_scalar!(
            records,
            field,
            len,
            RuntimeValue::as_f32,
            Float32Builder
        ),
        ParseAsType::F64 => build_fixed_size_list_column_for_scalar!(
            records,
            field,
            len,
            RuntimeValue::as_f64,
            Float64Builder
        ),
        ParseAsType::String => {
            let list_len = i32::try_from(len)
                .map_err(|_| format!("array field '{}' length is too large", field.name))?;
            let mut builder = FixedSizeListBuilder::new(StringBuilder::new(), list_len);
            for (row_index, record) in records.iter().enumerate() {
                let Some(values) = list_values_for_field(record, field, row_index)? else {
                    builder.append(false);
                    continue;
                };
                if values.len() != len as usize {
                    return Err(format!(
                        "record at row {row_index} field '{}' expected array length {}, got {}",
                        field.name,
                        len,
                        values.len()
                    ));
                }
                for value in values {
                    if let Some(value) = value.as_string() {
                        builder.values().append_value(value);
                    } else {
                        builder.values().append_null();
                    }
                }
                builder.append(true);
            }
            Ok(Arc::new(builder.finish()))
        }
        ParseAsType::Datetime => {
            let list_len = i32::try_from(len)
                .map_err(|_| format!("array field '{}' length is too large", field.name))?;
            let value_builder = TimestampNanosecondBuilder::new().with_data_type(
                ArrowDataType::Timestamp(ArrowTimeUnit::Nanosecond, Some("+00:00".into())),
            );
            let mut builder = FixedSizeListBuilder::new(value_builder, list_len);
            for (row_index, record) in records.iter().enumerate() {
                let Some(values) = list_values_for_field(record, field, row_index)? else {
                    builder.append(false);
                    continue;
                };
                if values.len() != len as usize {
                    return Err(format!(
                        "record at row {row_index} field '{}' expected array length {}, got {}",
                        field.name,
                        len,
                        values.len()
                    ));
                }
                for value in values {
                    if let Some(value) = value
                        .as_datetime()
                        .and_then(|value| value.timestamp_nanos_opt())
                    {
                        builder.values().append_value(value);
                    } else {
                        builder.values().append_null();
                    }
                }
                builder.append(true);
            }
            Ok(Arc::new(builder.finish()))
        }
        ParseAsType::Array { .. } | ParseAsType::Vec { .. } => Err(format!(
            "field '{}' uses unsupported array element type {:?}",
            field.name, element
        )),
    }
}

fn runtime_value_from_arrow_array(
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
        && let Some(item_ty) = parse_as_avro_item_type(&internal.ty)
    {
        return format!(r#"{{"type":"array","items":"{}"}}"#, item_ty);
    }
    format!(r#""{}""#, avro_type_name(field.ty))
}

fn parse_as_avro_item_type(ty: &ParseAsType) -> Option<&'static str> {
    let element = match ty {
        ParseAsType::Array { element, .. } | ParseAsType::Vec { element } => element.as_ref(),
        _ => return None,
    };
    match element {
        ParseAsType::Bool => Some("boolean"),
        ParseAsType::U8
        | ParseAsType::I8
        | ParseAsType::U16
        | ParseAsType::I16
        | ParseAsType::U32
        | ParseAsType::I32
        | ParseAsType::U64
        | ParseAsType::I64 => Some("long"),
        ParseAsType::F32 => Some("float"),
        ParseAsType::F64 => Some("double"),
        ParseAsType::String | ParseAsType::Datetime => Some("string"),
        ParseAsType::Array { .. } | ParseAsType::Vec { .. } => None,
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
    use std::sync::Arc;

    use chrono::DateTime;
    use nervix_models::{
        CodecJaqTransformations, CodecProtobufConfig, CreateCodec, CreateSchema, CreateWireSchema,
        Identifier, SchemaField,
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

    fn json_wire_schema() -> CreateWireSchemaStmt {
        CreateWireSchemaStmt::Json(CreateWireSchema {
            name: identifier("notification_wire"),
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

    fn avro_wire_schema() -> CreateWireSchemaStmt {
        CreateWireSchemaStmt::Avro(CreateWireSchema {
            name: identifier("notification_avro"),
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

    fn optional_json_wire_schema() -> CreateWireSchemaStmt {
        CreateWireSchemaStmt::Json(CreateWireSchema {
            name: identifier("optional_notification_wire"),
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

    fn optional_avro_wire_schema() -> CreateWireSchemaStmt {
        CreateWireSchemaStmt::Avro(CreateWireSchema {
            name: identifier("optional_notification_avro"),
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

    fn array_json_wire_schema() -> CreateWireSchemaStmt {
        CreateWireSchemaStmt::Json(CreateWireSchema {
            name: identifier("metrics_json"),
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

    fn array_avro_wire_schema() -> CreateWireSchemaStmt {
        CreateWireSchemaStmt::Avro(CreateWireSchema {
            name: identifier("metrics_avro"),
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

    fn array_record() -> RuntimeRecord {
        RuntimeRecord::from_fields([
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

    fn primitive_arrays_json_wire_schema() -> CreateWireSchemaStmt {
        CreateWireSchemaStmt::Json(CreateWireSchema {
            name: identifier("primitive_arrays_wire"),
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

    fn primitive_arrays_avro_wire_schema() -> CreateWireSchemaStmt {
        CreateWireSchemaStmt::Avro(CreateWireSchema {
            name: identifier("primitive_arrays_wire"),
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

    fn protobuf_descriptor(codec: &CreateCodec) -> ProtobufCodecDescriptor {
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
        ProtobufCodecDescriptor::from_file_descriptor_set(
            codec,
            file_descriptor_set,
            "nervix.test.Notification",
        )
        .expect("descriptor should be built")
    }

    fn primitive_arrays_record() -> RuntimeRecord {
        let mut fields = Vec::new();
        for (name, _, values, _) in primitive_array_cases() {
            fields.push((format!("{name}_array"), RuntimeValue::Array(values.clone())));
            fields.push((format!("{name}_vec"), RuntimeValue::Vec(values)));
        }
        RuntimeRecord::from_fields(fields)
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

    fn record() -> RuntimeRecord {
        RuntimeRecord::from_fields([
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
            ("latency".to_string(), RuntimeValue::F64(OrderedFloat(12.5))),
            ("active".to_string(), RuntimeValue::Bool(true)),
        ])
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
    fn runtime_records_roundtrip_through_arrow_batch() {
        let compiled = compile_schema(&schema());
        let records = vec![
            record(),
            RuntimeRecord::from_fields([
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

        let batch = compiled
            .arrow_batch_from_records(&records)
            .expect("records should convert to arrow");
        assert_eq!(batch.batch().num_rows(), 2);

        let roundtrip = compiled
            .decoded_records_from_arrow_batch(&batch)
            .expect("arrow batch should convert back to records");
        assert_eq!(roundtrip.len(), 2);
        assert_eq!(roundtrip[0].value("user_id"), Some(&RuntimeValue::U32(42)));
        assert_eq!(
            roundtrip[1].value("tenant"),
            Some(&RuntimeValue::String("beta".to_string()))
        );
    }

    #[test]
    fn optional_fields_roundtrip_through_arrow_batch_as_nulls() {
        let compiled = compile_schema(&optional_schema());
        assert!(compiled.arrow_schema().field(1).is_nullable());

        let batch = compiled
            .arrow_batch_from_records(&[RuntimeRecord::from_fields([(
                "user_id".to_string(),
                RuntimeValue::U32(42),
            )])])
            .expect("records should convert to arrow");
        let nickname = batch
            .batch()
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("nickname column should be strings");
        assert!(nickname.is_null(0));

        let roundtrip = compiled
            .decoded_records_from_arrow_batch(&batch)
            .expect("arrow batch should convert back to records");
        assert_eq!(roundtrip[0].value("user_id"), Some(&RuntimeValue::U32(42)));
        assert_eq!(roundtrip[0].value("nickname"), None);
    }

    #[test]
    fn runtime_arrow_batches_can_be_concatenated() {
        let compiled = compile_schema(&schema());
        let left = compiled
            .arrow_batch_from_records(&[record()])
            .expect("left batch should convert to arrow");
        let right_record = RuntimeRecord::from_fields([
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
        let right = compiled
            .arrow_batch_from_records(&[right_record])
            .expect("right batch should convert to arrow");

        let concatenated =
            RuntimeRecordBatch::concat(&[&left, &right]).expect("batches should concat");

        assert_eq!(concatenated.batch().num_rows(), 2);
        let roundtrip = compiled
            .decoded_records_from_arrow_batch(&concatenated)
            .expect("concatenated batch should convert back to records");
        assert_eq!(roundtrip[0].value("user_id"), Some(&RuntimeValue::U32(42)));
        assert_eq!(roundtrip[1].value("user_id"), Some(&RuntimeValue::U32(7)));
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
        let payload = encode_with_codec(&compiled_codec, &record()).expect("must encode");
        let decoded = decode_with_codec(&compiled_codec, &payload).expect("must decode");

        assert_eq!(decoded.value("user_id"), Some(&RuntimeValue::U32(42)));
        assert_eq!(
            decoded.value("tenant"),
            Some(&RuntimeValue::String("acme".to_string()))
        );
        assert_eq!(decoded.value("active"), Some(&RuntimeValue::Bool(true)));
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
        let payload = encode_with_codec(&compiled_codec, &record()).expect("must encode");
        let decoded = decode_with_codec(&compiled_codec, &payload).expect("must decode");

        assert_eq!(decoded.value("user_id"), Some(&RuntimeValue::U32(42)));
        assert_eq!(
            decoded.value("tenant"),
            Some(&RuntimeValue::String("acme".to_string()))
        );
        assert_eq!(
            decoded.value("latency"),
            Some(&RuntimeValue::F64(OrderedFloat(12.5)))
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
            decoded.value("cpu_last_64"),
            array_record().value("cpu_last_64")
        );
        assert_eq!(decoded.value("labels"), array_record().value("labels"));

        let batch = compiled_schema
            .arrow_batch_from_records(&[decoded.into_runtime_record(RuntimeRecordMetadata::test())])
            .expect("arrays should convert to Arrow");
        assert!(matches!(
            batch.batch().schema().field(0).data_type(),
            ArrowDataType::FixedSizeList(_, 3)
        ));
        assert!(matches!(
            batch.batch().schema().field(1).data_type(),
            ArrowDataType::List(_)
        ));

        let roundtrip = compiled_schema
            .decoded_records_from_arrow_batch(&batch)
            .expect("arrays should roundtrip from Arrow");
        assert_eq!(
            roundtrip[0].value("cpu_last_64"),
            array_record().value("cpu_last_64")
        );
        assert_eq!(roundtrip[0].value("labels"), array_record().value("labels"));
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

        let payload = encode_with_codec(&compiled_codec, &array_record()).expect("must encode");
        let decoded = decode_with_codec(&compiled_codec, &payload).expect("must decode");

        assert_eq!(
            decoded.value("cpu_last_64"),
            array_record().value("cpu_last_64")
        );
        assert_eq!(decoded.value("labels"), array_record().value("labels"));
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

        let payload = encode_with_codec(&compiled_codec, &array_record()).expect("must encode");
        let decoded = decode_with_codec(&compiled_codec, &payload).expect("must decode");

        assert_eq!(
            decoded.value("cpu_last_64"),
            array_record().value("cpu_last_64")
        );
        assert_eq!(decoded.value("labels"), array_record().value("labels"));
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

        let payload = encode_with_codec(&compiled_codec, &array_record()).expect("must encode");
        let decoded = decode_with_codec(&compiled_codec, &payload).expect("must decode");

        assert_eq!(
            decoded.value("cpu_last_64"),
            array_record().value("cpu_last_64")
        );
        assert_eq!(decoded.value("labels"), array_record().value("labels"));
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
                decoded.value(&field.name),
                expected.value(&field.name),
                "field {} should decode",
                field.name
            );
        }

        let batch = compiled_schema
            .arrow_batch_from_records(&[decoded.into_runtime_record(RuntimeRecordMetadata::test())])
            .expect("primitive arrays should convert to Arrow");
        let roundtrip = compiled_schema
            .decoded_records_from_arrow_batch(&batch)
            .expect("primitive arrays should roundtrip from Arrow");
        for field in compiled_schema.fields() {
            assert_eq!(
                roundtrip[0].value(&field.name),
                expected.value(&field.name),
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

        let payload = encode_with_codec(&compiled_codec, &expected).expect("must encode");
        let decoded = decode_with_codec(&compiled_codec, &payload).expect("must decode");

        for field in compiled_schema.fields() {
            assert_eq!(
                decoded.value(&field.name),
                expected.value(&field.name),
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

        let payload = encode_with_codec(&compiled_codec, &expected).expect("must encode");
        let decoded = decode_with_codec(&compiled_codec, &payload).expect("must decode");

        for field in compiled_schema.fields() {
            assert_eq!(
                decoded.value(&field.name),
                expected.value(&field.name),
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

        let payload = encode_with_codec(&compiled_codec, &expected).expect("must encode");
        let decoded = decode_with_codec(&compiled_codec, &payload).expect("must decode");

        for field in compiled_schema.fields() {
            assert_eq!(
                decoded.value(&field.name),
                expected.value(&field.name),
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
        assert_eq!(missing.value("user_id"), Some(&RuntimeValue::U32(42)));
        assert_eq!(missing.value("nickname"), None);

        let explicit_null = decode_with_codec(&compiled_codec, br#"{"user_id":7,"nickname":null}"#)
            .expect("null optional field should decode");
        assert_eq!(explicit_null.value("user_id"), Some(&RuntimeValue::U32(7)));
        assert_eq!(explicit_null.value("nickname"), None);
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

        let payload = encode_with_codec(
            &compiled_codec,
            &RuntimeRecord::from_fields([("user_id".to_string(), RuntimeValue::U32(42))]),
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

        let payload = encode_with_codec(
            &compiled_codec,
            &RuntimeRecord::from_fields([("user_id".to_string(), RuntimeValue::U32(42))]),
        )
        .expect("must encode");
        let decoded = decode_with_codec(&compiled_codec, &payload).expect("must decode");

        assert_eq!(decoded.value("user_id"), Some(&RuntimeValue::U32(42)));
        assert_eq!(decoded.value("nickname"), None);
    }

    #[test]
    fn avro_encode_rejects_incompatible_runtime_values() {
        let compiled_schema = Arc::new(compile_schema(&schema()));
        let compiled_codec = compile_codec(
            &codec("avro_codec"),
            compiled_schema,
            Some(&avro_wire_schema()),
        )
        .expect("codec should compile");
        let bad_record = RuntimeRecord::from_fields([
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
        ]);

        let err = encode_with_codec(&compiled_codec, &bad_record).expect_err("must reject");
        assert!(matches!(err, CodecError::EncodeField { field, .. } if field == "latency"));
    }

    #[test]
    fn runtime_record_remote_helpers_preserve_semantics() {
        let record = record().with_ingested_at_watermarks(Timestamp::from_unix_nanos(1_234_567));
        assert_eq!(
            record.to_json_string(),
            r#"{"active":true,"created_at":"2025-01-02T03:04:05+00:00","latency":12.5,"tenant":"acme","user_id":42}"#
        );

        let remote = record.to_remote();
        let roundtrip = RuntimeRecord::from_remote(remote);
        assert_eq!(
            roundtrip.value("tenant"),
            Some(&RuntimeValue::String("acme".to_string()))
        );
        assert_eq!(roundtrip.value("user_id"), Some(&RuntimeValue::U32(42)));
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

        let missing_wire_schema = CreateWireSchemaStmt::Json(CreateWireSchema {
            name: identifier("notification_wire_partial"),
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
    fn json_and_avro_encode_report_missing_runtime_fields() {
        let compiled_schema = Arc::new(compile_schema(&schema()));
        let json_codec = compile_codec(
            &codec("json_codec"),
            compiled_schema.clone(),
            Some(&json_wire_schema()),
        )
        .expect("json codec should compile");
        let avro_codec = compile_codec(
            &codec("avro_codec"),
            compiled_schema,
            Some(&avro_wire_schema()),
        )
        .expect("avro codec should compile");

        let partial = RuntimeRecord::from_fields([
            ("user_id".to_string(), RuntimeValue::U32(42)),
            (
                "tenant".to_string(),
                RuntimeValue::String("acme".to_string()),
            ),
        ]);

        let json_err = encode_with_codec(&json_codec, &partial).expect_err("json must reject");
        assert!(matches!(json_err, CodecError::EncodeField { field, .. } if field == "created_at"));

        let avro_err = encode_with_codec(&avro_codec, &partial).expect_err("avro must reject");
        assert!(matches!(avro_err, CodecError::EncodeField { field, .. } if field == "created_at"));
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

        assert_eq!(decoded.value("user_id"), Some(&RuntimeValue::U32(42)));
        assert_eq!(
            decoded.value("tenant"),
            Some(&RuntimeValue::String("acme".to_string()))
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

        let payload = encode_with_codec(&compiled_codec, &record()).expect("must encode");
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
        let compiled_codec = compile_codec_with_protobuf(
            &codec,
            compiled_schema,
            None,
            Some(protobuf_descriptor(&codec)),
        )
        .expect("codec should compile");
        assert!(compiled_codec.requires_blocking_decode());
        assert!(!compiled_codec.requires_blocking_encode());

        let payload = [
            0x08, 42, 0x12, 4, b'a', b'c', b'm', b'e', 0x1a, 5, b'h', b'e', b'l', b'l', b'o',
        ];
        let decoded = decode_with_codec(&compiled_codec, &payload).expect("must decode");

        assert_eq!(decoded.value("user_id"), Some(&RuntimeValue::U32(42)));
        assert_eq!(
            decoded.value("tenant"),
            Some(&RuntimeValue::String("acme".to_string()))
        );
        assert_eq!(
            decoded.value("payload"),
            Some(&RuntimeValue::String("hello".to_string()))
        );
    }

    #[test]
    fn protobuf_codec_applies_transformation_on_emitting_before_encoding() {
        let codec = protobuf_codec("protobuf_emit", None, Some("."));
        let compiled_schema = Arc::new(compile_schema(&protobuf_schema()));
        let compiled_codec = compile_codec_with_protobuf(
            &codec,
            compiled_schema,
            None,
            Some(protobuf_descriptor(&codec)),
        )
        .expect("codec should compile");
        assert!(!compiled_codec.requires_blocking_decode());
        assert!(compiled_codec.requires_blocking_encode());

        let record = RuntimeRecord::from_fields([
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
        let payload = encode_with_codec(&compiled_codec, &record).expect("must encode");

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
        let payload = encode_with_codec(&compiled_codec, &record()).expect("must encode");
        let decoded = decode_with_codec(&compiled_codec, &payload).expect("must decode");

        assert_eq!(decoded.value("user_id"), Some(&RuntimeValue::U32(42)));
        assert_eq!(
            decoded.value("tenant"),
            Some(&RuntimeValue::String("acme".to_string()))
        );
        assert_eq!(decoded.value("active"), Some(&RuntimeValue::Bool(true)));
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
        let payload = encode_with_codec(&compiled_codec, &record()).expect("must encode");

        assert_eq!(
            String::from_utf8(payload).expect("xml must be utf8"),
            "<notification><user_id>42</user_id><tenant>acme</tenant></notification>"
        );
    }
}
