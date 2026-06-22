use serde::{Deserialize, Serialize};
use strum::AsRefStr;

use crate::Identifier;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CreateWireSchemaStmt {
    Json(CreateWireSchema<JsonType>),
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
pub type CreateAvroWireSchema = CreateWireSchema<AvroType>;

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    Array { element: Box<ParseAsType>, len: u32 },
    Vec { element: Box<ParseAsType> },
}
