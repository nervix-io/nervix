use error_stack::Report;
use nervix_models::{
    AvroType, CreateIngestor, CreateRelay, CreateSchema, CreateSchemaStmt, CreateTransportKafka,
    Identifier, IngestSource, JsonType, KafkaConfigEntry, KafkaIngestMode, KafkaOffsetMode,
    MaterializedStreamState, Model, NameError, ParseAsType, RelayBranching, SchemaField,
};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum StoredModelEnvelope {
    V1(StoredModelVersioned),
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum StoredModelVersioned {
    Schema(StoredCreateSchemaStmt),
    TransportKafka(StoredCreateTransportKafka),
    Ingestor(StoredCreateIngestor),
    Stream(StoredCreateRelay),
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum StoredCreateSchemaStmt {
    Json(StoredCreateSchema<StoredJsonType>),
    Avro(StoredCreateSchema<StoredAvroType>),
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredCreateSchema<T> {
    pub name: String,
    pub fields: Vec<StoredSchemaField<T>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredSchemaField<T> {
    pub name: String,
    pub ty: T,
    pub parse_as: Option<StoredParseAsType>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum StoredJsonType {
    String,
    Number,
    Integer,
    Object,
    Array,
    Boolean,
    Null,
    U8,
    I8,
    U16,
    I16,
    U32,
    I32,
    U64,
    I64,
    Datetime,
    F32,
    F64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum StoredAvroType {
    Null,
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Bytes,
    String,
    Record,
    Enum,
    Array,
    Map,
    Fixed,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
#[rkyv(serialize_bounds(
    __S: rkyv::ser::Writer + rkyv::ser::Allocator,
    __S::Error: rkyv::rancor::Source,
))]
#[rkyv(deserialize_bounds(__D::Error: rkyv::rancor::Source))]
#[rkyv(bytecheck(bounds(__C: rkyv::validation::ArchiveContext)))]
pub enum StoredParseAsType {
    U8,
    I8,
    U16,
    I16,
    U32,
    I32,
    U64,
    I64,
    Bool,
    String,
    Datetime,
    F32,
    F64,
    Array {
        #[rkyv(omit_bounds)]
        element: Box<StoredParseAsType>,
        len: u32,
    },
    Vec {
        #[rkyv(omit_bounds)]
        element: Box<StoredParseAsType>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredCreateTransportKafka {
    pub name: String,
    pub config: Vec<StoredKafkaConfigEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredKafkaConfigEntry {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredCreateIngestor {
    pub name: String,
    pub into_relay: String,
    pub decode_using_codec: String,
    pub timestamp_source: Option<StoredIngestTimestampSource>,
    pub branched_by: Vec<String>,
    pub source: StoredIngestSource,
    pub filter_map: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum StoredIngestSource {
    Kafka {
        client: String,
        topic: String,
        offset_mode: StoredKafkaOffsetMode,
        instances: u64,
        mode: StoredKafkaIngestMode,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum StoredKafkaOffsetMode {
    ConsumerGroup(String),
    Domain,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum StoredIngestTimestampSource {
    Now,
    At(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum StoredKafkaIngestMode {
    AckParallel {
        max: u64,
        batch_timeout: String,
        timeout: String,
        retry_backoff: String,
        retry_max_backoff: String,
    },
    AckSequential {
        timeout: String,
        retry_backoff: String,
        retry_max_backoff: String,
    },
    NoAckParallel {
        max: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StoredCreateRelay {
    pub name: String,
    pub schema: String,
    pub buffer: u64,
    pub branching: StoredRelayBranching,
    pub materialized_state: Option<StoredMaterializedStreamState>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum StoredRelayBranching {
    BranchedBy { branch: String },
    Unbranched,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum StoredMaterializedStreamState {
    LastByTimestamp,
}

impl From<Model> for StoredModelVersioned {
    fn from(value: Model) -> Self {
        match value {
            Model::Schema(v) => Self::Schema(v.into()),
            Model::TransportKafka(v) => Self::TransportKafka(v.into()),
            Model::Ingestor(v) => Self::Ingestor(v.into()),
            Model::Stream(v) => Self::Stream(v.into()),
        }
    }
}

impl TryFrom<StoredModelEnvelope> for Model {
    type Error = Report<NameError>;

    fn try_from(value: StoredModelEnvelope) -> Result<Self, Self::Error> {
        match value {
            StoredModelEnvelope::V1(v) => v.try_into(),
        }
    }
}

impl TryFrom<StoredModelVersioned> for Model {
    type Error = Report<NameError>;

    fn try_from(value: StoredModelVersioned) -> Result<Self, Self::Error> {
        match value {
            StoredModelVersioned::Schema(v) => Ok(Model::Schema(v.try_into()?)),
            StoredModelVersioned::TransportKafka(v) => Ok(Model::TransportKafka(v.try_into()?)),
            StoredModelVersioned::Ingestor(v) => Ok(Model::Ingestor(v.try_into()?)),
            StoredModelVersioned::Stream(v) => Ok(Model::Stream(v.try_into()?)),
        }
    }
}

impl From<CreateSchemaStmt> for StoredCreateSchemaStmt {
    fn from(value: CreateSchemaStmt) -> Self {
        match value {
            CreateSchemaStmt::Json(v) => Self::Json(v.into()),
            CreateSchemaStmt::Avro(v) => Self::Avro(v.into()),
        }
    }
}

impl TryFrom<StoredCreateSchemaStmt> for CreateSchemaStmt {
    type Error = Report<NameError>;

    fn try_from(value: StoredCreateSchemaStmt) -> Result<Self, Self::Error> {
        match value {
            StoredCreateSchemaStmt::Json(v) => Ok(Self::Json(v.try_into()?)),
            StoredCreateSchemaStmt::Avro(v) => Ok(Self::Avro(v.try_into()?)),
        }
    }
}

impl From<CreateSchema<JsonType>> for StoredCreateSchema<StoredJsonType> {
    fn from(value: CreateSchema<JsonType>) -> Self {
        Self {
            name: value.name.to_string(),
            fields: value.fields.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<CreateSchema<AvroType>> for StoredCreateSchema<StoredAvroType> {
    fn from(value: CreateSchema<AvroType>) -> Self {
        Self {
            name: value.name.to_string(),
            fields: value.fields.into_iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<StoredCreateSchema<StoredJsonType>> for CreateSchema<JsonType> {
    type Error = Report<NameError>;

    fn try_from(value: StoredCreateSchema<StoredJsonType>) -> Result<Self, Self::Error> {
        Ok(Self {
            name: Identifier::parse(&value.name)?,
            fields: value
                .fields
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}

impl TryFrom<StoredCreateSchema<StoredAvroType>> for CreateSchema<AvroType> {
    type Error = Report<NameError>;

    fn try_from(value: StoredCreateSchema<StoredAvroType>) -> Result<Self, Self::Error> {
        Ok(Self {
            name: Identifier::parse(&value.name)?,
            fields: value
                .fields
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}

impl From<SchemaField<JsonType>> for StoredSchemaField<StoredJsonType> {
    fn from(value: SchemaField<JsonType>) -> Self {
        Self {
            name: value.name.to_string(),
            ty: value.ty.into(),
            parse_as: value.parse_as.map(Into::into),
        }
    }
}

impl From<SchemaField<AvroType>> for StoredSchemaField<StoredAvroType> {
    fn from(value: SchemaField<AvroType>) -> Self {
        Self {
            name: value.name.to_string(),
            ty: value.ty.into(),
            parse_as: value.parse_as.map(Into::into),
        }
    }
}

impl TryFrom<StoredSchemaField<StoredJsonType>> for SchemaField<JsonType> {
    type Error = Report<NameError>;

    fn try_from(value: StoredSchemaField<StoredJsonType>) -> Result<Self, Self::Error> {
        Ok(Self {
            name: Identifier::parse(&value.name)?,
            ty: value.ty.into(),
            parse_as: value.parse_as.map(Into::into),
        })
    }
}

impl TryFrom<StoredSchemaField<StoredAvroType>> for SchemaField<AvroType> {
    type Error = Report<NameError>;

    fn try_from(value: StoredSchemaField<StoredAvroType>) -> Result<Self, Self::Error> {
        Ok(Self {
            name: Identifier::parse(&value.name)?,
            ty: value.ty.into(),
            parse_as: value.parse_as.map(Into::into),
        })
    }
}

impl From<JsonType> for StoredJsonType {
    fn from(value: JsonType) -> Self {
        match value {
            JsonType::String => Self::String,
            JsonType::Number => Self::Number,
            JsonType::Integer => Self::Integer,
            JsonType::Object => Self::Object,
            JsonType::Array => Self::Array,
            JsonType::Boolean => Self::Boolean,
            JsonType::Null => Self::Null,
            JsonType::U8 => Self::U8,
            JsonType::I8 => Self::I8,
            JsonType::U16 => Self::U16,
            JsonType::I16 => Self::I16,
            JsonType::U32 => Self::U32,
            JsonType::I32 => Self::I32,
            JsonType::U64 => Self::U64,
            JsonType::I64 => Self::I64,
            JsonType::Datetime => Self::Datetime,
            JsonType::F32 => Self::F32,
            JsonType::F64 => Self::F64,
        }
    }
}

impl From<StoredJsonType> for JsonType {
    fn from(value: StoredJsonType) -> Self {
        match value {
            StoredJsonType::String => Self::String,
            StoredJsonType::Number => Self::Number,
            StoredJsonType::Integer => Self::Integer,
            StoredJsonType::Object => Self::Object,
            StoredJsonType::Array => Self::Array,
            StoredJsonType::Boolean => Self::Boolean,
            StoredJsonType::Null => Self::Null,
            StoredJsonType::U8 => Self::U8,
            StoredJsonType::I8 => Self::I8,
            StoredJsonType::U16 => Self::U16,
            StoredJsonType::I16 => Self::I16,
            StoredJsonType::U32 => Self::U32,
            StoredJsonType::I32 => Self::I32,
            StoredJsonType::U64 => Self::U64,
            StoredJsonType::I64 => Self::I64,
            StoredJsonType::Datetime => Self::Datetime,
            StoredJsonType::F32 => Self::F32,
            StoredJsonType::F64 => Self::F64,
        }
    }
}

impl From<AvroType> for StoredAvroType {
    fn from(value: AvroType) -> Self {
        match value {
            AvroType::Null => Self::Null,
            AvroType::Boolean => Self::Boolean,
            AvroType::Int => Self::Int,
            AvroType::Long => Self::Long,
            AvroType::Float => Self::Float,
            AvroType::Double => Self::Double,
            AvroType::Bytes => Self::Bytes,
            AvroType::String => Self::String,
            AvroType::Record => Self::Record,
            AvroType::Enum => Self::Enum,
            AvroType::Array => Self::Array,
            AvroType::Map => Self::Map,
            AvroType::Fixed => Self::Fixed,
        }
    }
}

impl From<StoredAvroType> for AvroType {
    fn from(value: StoredAvroType) -> Self {
        match value {
            StoredAvroType::Null => Self::Null,
            StoredAvroType::Boolean => Self::Boolean,
            StoredAvroType::Int => Self::Int,
            StoredAvroType::Long => Self::Long,
            StoredAvroType::Float => Self::Float,
            StoredAvroType::Double => Self::Double,
            StoredAvroType::Bytes => Self::Bytes,
            StoredAvroType::String => Self::String,
            StoredAvroType::Record => Self::Record,
            StoredAvroType::Enum => Self::Enum,
            StoredAvroType::Array => Self::Array,
            StoredAvroType::Map => Self::Map,
            StoredAvroType::Fixed => Self::Fixed,
        }
    }
}

impl From<ParseAsType> for StoredParseAsType {
    fn from(value: ParseAsType) -> Self {
        match value {
            ParseAsType::U8 => Self::U8,
            ParseAsType::I8 => Self::I8,
            ParseAsType::U16 => Self::U16,
            ParseAsType::I16 => Self::I16,
            ParseAsType::U32 => Self::U32,
            ParseAsType::I32 => Self::I32,
            ParseAsType::U64 => Self::U64,
            ParseAsType::I64 => Self::I64,
            ParseAsType::Bool => Self::Bool,
            ParseAsType::String => Self::String,
            ParseAsType::Datetime => Self::Datetime,
            ParseAsType::F32 => Self::F32,
            ParseAsType::F64 => Self::F64,
            ParseAsType::Array { element, len } => Self::Array {
                element: Box::new((*element).into()),
                len,
            },
            ParseAsType::Vec { element } => Self::Vec {
                element: Box::new((*element).into()),
            },
        }
    }
}

impl From<StoredParseAsType> for ParseAsType {
    fn from(value: StoredParseAsType) -> Self {
        match value {
            StoredParseAsType::U8 => Self::U8,
            StoredParseAsType::I8 => Self::I8,
            StoredParseAsType::U16 => Self::U16,
            StoredParseAsType::I16 => Self::I16,
            StoredParseAsType::U32 => Self::U32,
            StoredParseAsType::I32 => Self::I32,
            StoredParseAsType::U64 => Self::U64,
            StoredParseAsType::I64 => Self::I64,
            StoredParseAsType::Bool => Self::Bool,
            StoredParseAsType::String => Self::String,
            StoredParseAsType::Datetime => Self::Datetime,
            StoredParseAsType::F32 => Self::F32,
            StoredParseAsType::F64 => Self::F64,
            StoredParseAsType::Array { element, len } => Self::Array {
                element: Box::new((*element).into()),
                len,
            },
            StoredParseAsType::Vec { element } => Self::Vec {
                element: Box::new((*element).into()),
            },
        }
    }
}

impl From<CreateTransportKafka> for StoredCreateTransportKafka {
    fn from(value: CreateTransportKafka) -> Self {
        Self {
            name: value.name.to_string(),
            config: value.config.into_iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<StoredCreateTransportKafka> for CreateTransportKafka {
    type Error = Report<NameError>;

    fn try_from(value: StoredCreateTransportKafka) -> Result<Self, Self::Error> {
        Ok(Self {
            name: Identifier::parse(&value.name)?,
            config: value.config.into_iter().map(Into::into).collect(),
        })
    }
}

impl From<KafkaConfigEntry> for StoredKafkaConfigEntry {
    fn from(value: KafkaConfigEntry) -> Self {
        Self {
            key: value.key,
            value: value.value,
        }
    }
}

impl From<StoredKafkaConfigEntry> for KafkaConfigEntry {
    fn from(value: StoredKafkaConfigEntry) -> Self {
        Self {
            key: value.key,
            value: value.value,
        }
    }
}

impl From<CreateIngestor> for StoredCreateIngestor {
    fn from(value: CreateIngestor) -> Self {
        Self {
            name: value.name.to_string(),
            into_relay: value.into_relay.to_string(),
            decode_using_codec: value.decode_using_codec.to_string(),
            timestamp_source: value.timestamp_source.map(Into::into),
            branched_by: value
                .branched_by
                .into_iter()
                .map(|x| x.to_string())
                .collect(),
            source: value.source.into(),
            filter_map: value.filter_map,
        }
    }
}

impl TryFrom<StoredCreateIngestor> for CreateIngestor {
    type Error = Report<NameError>;

    fn try_from(value: StoredCreateIngestor) -> Result<Self, Self::Error> {
        Ok(Self {
            name: Identifier::parse(&value.name)?,
            into_relay: Identifier::parse(&value.into_relay)?,
            decode_using_codec: Identifier::parse(&value.decode_using_codec)?,
            timestamp_source: value.timestamp_source.map(TryInto::try_into).transpose()?,
            branched_by: value
                .branched_by
                .into_iter()
                .map(|x| Identifier::parse(&x))
                .collect::<Result<Vec<_>, _>>()?,
            source: value.source.try_into()?,
            filter_map: value.filter_map,
        })
    }
}

impl From<IngestSource> for StoredIngestSource {
    fn from(value: IngestSource) -> Self {
        match value {
            IngestSource::Kafka {
                client,
                topic,
                offset_mode,
                instances,
                mode,
            } => Self::Kafka {
                client: client.to_string(),
                topic: topic.to_string(),
                offset_mode: offset_mode.into(),
                instances,
                mode: mode.into(),
            },
        }
    }
}

impl TryFrom<StoredIngestSource> for IngestSource {
    type Error = Report<NameError>;

    fn try_from(value: StoredIngestSource) -> Result<Self, Self::Error> {
        match value {
            StoredIngestSource::Kafka {
                client,
                topic,
                offset_mode,
                instances,
                mode,
            } => Ok(Self::Kafka {
                client: Identifier::parse(&client)?,
                topic: Identifier::parse(&topic)?,
                offset_mode: offset_mode.try_into()?,
                instances,
                mode: mode.into(),
            }),
        }
    }
}

impl From<nervix_models::IngestTimestampSource> for StoredIngestTimestampSource {
    fn from(value: nervix_models::IngestTimestampSource) -> Self {
        match value {
            nervix_models::IngestTimestampSource::Now => Self::Now,
            nervix_models::IngestTimestampSource::At(field) => Self::At(field.to_string()),
        }
    }
}

impl TryFrom<StoredIngestTimestampSource> for nervix_models::IngestTimestampSource {
    type Error = Report<NameError>;

    fn try_from(value: StoredIngestTimestampSource) -> Result<Self, Self::Error> {
        match value {
            StoredIngestTimestampSource::Now => Ok(Self::Now),
            StoredIngestTimestampSource::At(field) => Ok(Self::At(Identifier::parse(&field)?)),
        }
    }
}

impl From<KafkaOffsetMode> for StoredKafkaOffsetMode {
    fn from(value: KafkaOffsetMode) -> Self {
        match value {
            KafkaOffsetMode::ConsumerGroup(group) => Self::ConsumerGroup(group.to_string()),
            KafkaOffsetMode::Domain => Self::Domain,
        }
    }
}

impl TryFrom<StoredKafkaOffsetMode> for KafkaOffsetMode {
    type Error = Report<NameError>;

    fn try_from(value: StoredKafkaOffsetMode) -> Result<Self, Self::Error> {
        match value {
            StoredKafkaOffsetMode::ConsumerGroup(group) => {
                Ok(Self::ConsumerGroup(Identifier::parse(&group)?))
            }
            StoredKafkaOffsetMode::Domain => Ok(Self::Domain),
        }
    }
}

impl From<KafkaIngestMode> for StoredKafkaIngestMode {
    fn from(value: KafkaIngestMode) -> Self {
        match value {
            KafkaIngestMode::AckParallel {
                max,
                batch_timeout,
                timeout,
                retry_policy,
            } => Self::AckParallel {
                max,
                batch_timeout,
                timeout,
                retry_backoff: retry_policy.backoff,
                retry_max_backoff: retry_policy.max_backoff,
            },
            KafkaIngestMode::AckSequential {
                timeout,
                retry_policy,
            } => Self::AckSequential {
                timeout,
                retry_backoff: retry_policy.backoff,
                retry_max_backoff: retry_policy.max_backoff,
            },
            KafkaIngestMode::NoAckParallel { max } => Self::NoAckParallel { max },
        }
    }
}

impl From<StoredKafkaIngestMode> for KafkaIngestMode {
    fn from(value: StoredKafkaIngestMode) -> Self {
        match value {
            StoredKafkaIngestMode::AckParallel {
                max,
                batch_timeout,
                timeout,
                retry_backoff,
                retry_max_backoff,
            } => Self::AckParallel {
                max,
                batch_timeout,
                timeout,
                retry_policy: nervix_models::RetryPolicy {
                    backoff: retry_backoff,
                    max_backoff: retry_max_backoff,
                },
            },
            StoredKafkaIngestMode::AckSequential {
                timeout,
                retry_backoff,
                retry_max_backoff,
            } => Self::AckSequential {
                timeout,
                retry_policy: nervix_models::RetryPolicy {
                    backoff: retry_backoff,
                    max_backoff: retry_max_backoff,
                },
            },
            StoredKafkaIngestMode::NoAckParallel { max } => Self::NoAckParallel { max },
        }
    }
}

impl From<CreateRelay> for StoredCreateRelay {
    fn from(value: CreateRelay) -> Self {
        let branching = match value.branching {
            RelayBranching::BranchedBy { branch } => {
                StoredRelayBranching::BranchedBy {
                    branch: branch.to_string(),
                }
            }
            RelayBranching::Unbranched => StoredRelayBranching::Unbranched,
        };
        Self {
            name: value.name.to_string(),
            schema: value.schema.to_string(),
            buffer: value.buffer as u64,
            branching,
            materialized_state: value.materialized_state.map(Into::into),
        }
    }
}

impl TryFrom<StoredCreateRelay> for CreateRelay {
    type Error = Report<NameError>;

    fn try_from(value: StoredCreateRelay) -> Result<Self, Self::Error> {
        let branching = match value.branching {
            StoredRelayBranching::BranchedBy { branch } => {
                RelayBranching::branched_by(Identifier::parse(&branch)?)
            }
            StoredRelayBranching::Unbranched => RelayBranching::unbranched(),
        };
        Ok(Self {
            name: Identifier::parse(&value.name)?,
            schema: Identifier::parse(&value.schema)?,
            buffer: value.buffer as usize,
            branching,
            materialized_state: value.materialized_state.map(Into::into),
        })
    }
}

impl From<MaterializedStreamState> for StoredMaterializedStreamState {
    fn from(value: MaterializedStreamState) -> Self {
        match value {
            MaterializedStreamState::LastByTimestamp => Self::LastByTimestamp,
        }
    }
}

impl From<StoredMaterializedStreamState> for MaterializedStreamState {
    fn from(value: StoredMaterializedStreamState) -> Self {
        match value {
            StoredMaterializedStreamState::LastByTimestamp => Self::LastByTimestamp,
        }
    }
}
