use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use strum::AsRefStr;

use crate::Identifier;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CreateWireSchemaStmt {
    Json(CreateWireSchema<JsonType>),
    Cbor(CreateWireSchema<CborType>),
    Avro(CreateWireSchema<AvroType>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateSchema {
    pub name: Identifier,
    pub fields: Vec<SchemaField>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaField {
    pub name: Identifier,
    pub ty: ParseAsType,
    #[serde(default)]
    pub optional: bool,
    #[serde(default)]
    pub sensitive: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateWireSchema<T> {
    pub name: Identifier,
    #[serde(default)]
    pub strictness: WireSchemaStrictness,
    pub fields: Vec<WireSchemaField<T>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireSchemaField<T> {
    pub name: Identifier,
    pub ty: T,
    #[serde(default)]
    pub optional: bool,
}

pub type CreateJsonWireSchema = CreateWireSchema<JsonType>;
pub type CreateCborWireSchema = CreateWireSchema<CborType>;
pub type CreateAvroWireSchema = CreateWireSchema<AvroType>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, AsRefStr, Default)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum WireSchemaStrictness {
    #[default]
    Strict,
    Loose,
}

impl WireSchemaStrictness {
    pub fn allows_unknown_fields(self) -> bool {
        match self {
            Self::Strict => false,
            Self::Loose => true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, AsRefStr)]
#[strum(serialize_all = "lowercase")]
pub enum JsonType {
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

pub type CborType = JsonType;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, AsRefStr)]
#[strum(serialize_all = "lowercase")]
pub enum AvroType {
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

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
#[rkyv(serialize_bounds(
    __S: rkyv::ser::Writer + rkyv::ser::Allocator,
    __S::Error: rkyv::rancor::Source,
))]
#[rkyv(deserialize_bounds(__D::Error: rkyv::rancor::Source))]
#[rkyv(bytecheck(bounds(__C: rkyv::validation::ArchiveContext)))]
pub enum ParseAsType {
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
        element: Box<ParseAsType>,
        len: u32,
    },
    Vec {
        #[rkyv(omit_bounds)]
        element: Box<ParseAsType>,
    },
}

impl std::fmt::Display for ParseAsType {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::U8 => formatter.write_str("U8"),
            Self::I8 => formatter.write_str("I8"),
            Self::U16 => formatter.write_str("U16"),
            Self::I16 => formatter.write_str("I16"),
            Self::U32 => formatter.write_str("U32"),
            Self::I32 => formatter.write_str("I32"),
            Self::U64 => formatter.write_str("U64"),
            Self::I64 => formatter.write_str("I64"),
            Self::F32 => formatter.write_str("F32"),
            Self::F64 => formatter.write_str("F64"),
            Self::Bool => formatter.write_str("BOOL"),
            Self::String => formatter.write_str("STRING"),
            Self::Datetime => formatter.write_str("DATETIME"),
            Self::Vec { element } => write!(formatter, "VEC<{element}>"),
            Self::Array { element, len } => write!(formatter, "ARRAY<{element}, {len}>"),
        }
    }
}
