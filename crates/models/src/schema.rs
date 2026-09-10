use std::num::NonZeroU32;

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use strum::AsRefStr;
use thiserror::Error;

use crate::{FieldName, SchemaName, WireSchemaName};

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CreateSchema {
    pub name: SchemaName,
    pub fields: Vec<SchemaField>,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct AlterSchema {
    pub schema: SchemaName,
    pub operations: Vec<AlterSchemaOperation>,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum AlterSchemaOperation {
    AddField { field: SchemaField },
    DropField { field: FieldName },
    RenameField { field: FieldName, to: FieldName },
    SetFieldType { field: FieldName, ty: ParseAsType },
    SetFieldOptional { field: FieldName, optional: bool },
    SetFieldSensitive { field: FieldName, sensitive: bool },
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct SchemaField {
    pub name: FieldName,
    pub ty: ParseAsType,
    #[serde(default)]
    pub optional: bool,
    #[serde(default)]
    pub sensitive: bool,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CreateWireSchema<T> {
    pub name: WireSchemaName,
    #[serde(default)]
    pub strictness: WireSchemaStrictness,
    pub fields: Vec<WireSchemaField<T>>,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct AlterWireSchema<T> {
    pub schema: WireSchemaName,
    pub operations: Vec<AlterWireSchemaOperation<T>>,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum AlterWireSchemaOperation<T> {
    SetMode { mode: WireSchemaStrictness },
    AddField { field: WireSchemaField<T> },
    DropField { field: FieldName },
    RenameField { field: FieldName, to: FieldName },
    SetFieldType { field: FieldName, ty: T },
    SetFieldOptional { field: FieldName, optional: bool },
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct WireSchemaField<T> {
    pub name: FieldName,
    pub ty: T,
    #[serde(default)]
    pub optional: bool,
}

pub type CreateJsonWireSchema = CreateWireSchema<JsonType>;
pub type CreateCborWireSchema = CreateWireSchema<CborType>;
pub type CreateAvroWireSchema = CreateWireSchema<AvroType>;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AlterSchemaError {
    #[error("ALTER targets schema `{requested}`, but the stored schema is `{stored}`")]
    SchemaNameMismatch {
        stored: SchemaName,
        requested: SchemaName,
    },
    #[error("field `{field}` already exists")]
    FieldAlreadyExists { field: FieldName },
    #[error("field `{field}` does not exist")]
    FieldNotFound { field: FieldName },
    #[error("cannot rename field to `{field}` because that field already exists")]
    RenameTargetAlreadyExists { field: FieldName },
    #[error("a schema must retain at least one field")]
    CannotDropLastField,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AlterWireSchemaError {
    #[error("ALTER targets wire schema `{requested}`, but the stored wire schema is `{stored}`")]
    SchemaNameMismatch {
        stored: WireSchemaName,
        requested: WireSchemaName,
    },
    #[error("field `{field}` already exists")]
    FieldAlreadyExists { field: FieldName },
    #[error("field `{field}` does not exist")]
    FieldNotFound { field: FieldName },
    #[error("cannot rename field to `{field}` because that field already exists")]
    RenameTargetAlreadyExists { field: FieldName },
    #[error("a schema must retain at least one field")]
    CannotDropLastField,
}

impl CreateSchema {
    pub fn apply_alter(&mut self, alter: &AlterSchema) -> Result<(), AlterSchemaError> {
        if self.name != alter.schema {
            return Err(AlterSchemaError::SchemaNameMismatch {
                stored: self.name.clone(),
                requested: alter.schema.clone(),
            });
        }

        let mut candidate = self.clone();
        for operation in &alter.operations {
            candidate.apply_alter_operation(operation)?;
        }
        *self = candidate;
        Ok(())
    }

    fn apply_alter_operation(
        &mut self,
        operation: &AlterSchemaOperation,
    ) -> Result<(), AlterSchemaError> {
        match operation {
            AlterSchemaOperation::AddField { field } => {
                self.ensure_field_absent(&field.name)?;
                self.fields.push(field.clone());
            }
            AlterSchemaOperation::DropField { field } => {
                let index = self.field_index(field)?;
                if self.fields.len() == 1 {
                    return Err(AlterSchemaError::CannotDropLastField);
                }
                self.fields.remove(index);
            }
            AlterSchemaOperation::RenameField { field, to } => {
                let index = self.field_index(field)?;
                self.ensure_rename_target_absent(field, to)?;
                self.fields[index].name = to.clone();
            }
            AlterSchemaOperation::SetFieldType { field, ty } => {
                let index = self.field_index(field)?;
                self.fields[index].ty = ty.clone();
            }
            AlterSchemaOperation::SetFieldOptional { field, optional } => {
                let index = self.field_index(field)?;
                self.fields[index].optional = *optional;
            }
            AlterSchemaOperation::SetFieldSensitive { field, sensitive } => {
                let index = self.field_index(field)?;
                self.fields[index].sensitive = *sensitive;
            }
        }
        Ok(())
    }

    fn field_index(&self, field: &FieldName) -> Result<usize, AlterSchemaError> {
        self.fields
            .iter()
            .position(|candidate| candidate.name == *field)
            .ok_or_else(|| AlterSchemaError::FieldNotFound {
                field: field.clone(),
            })
    }

    fn ensure_field_absent(&self, field: &FieldName) -> Result<(), AlterSchemaError> {
        if self.fields.iter().any(|candidate| candidate.name == *field) {
            return Err(AlterSchemaError::FieldAlreadyExists {
                field: field.clone(),
            });
        }
        Ok(())
    }

    fn ensure_rename_target_absent(
        &self,
        source: &FieldName,
        target: &FieldName,
    ) -> Result<(), AlterSchemaError> {
        if source != target
            && self
                .fields
                .iter()
                .any(|candidate| candidate.name == *target)
        {
            return Err(AlterSchemaError::RenameTargetAlreadyExists {
                field: target.clone(),
            });
        }
        Ok(())
    }
}

impl<T> CreateWireSchema<T>
where
    T: Clone,
{
    pub fn apply_alter(&mut self, alter: &AlterWireSchema<T>) -> Result<(), AlterWireSchemaError> {
        if self.name != alter.schema {
            return Err(AlterWireSchemaError::SchemaNameMismatch {
                stored: self.name.clone(),
                requested: alter.schema.clone(),
            });
        }

        let mut candidate = self.clone();
        for operation in &alter.operations {
            candidate.apply_alter_operation(operation)?;
        }
        *self = candidate;
        Ok(())
    }

    fn apply_alter_operation(
        &mut self,
        operation: &AlterWireSchemaOperation<T>,
    ) -> Result<(), AlterWireSchemaError> {
        match operation {
            AlterWireSchemaOperation::SetMode { mode } => {
                self.strictness = *mode;
            }
            AlterWireSchemaOperation::AddField { field } => {
                self.ensure_field_absent(&field.name)?;
                self.fields.push(field.clone());
            }
            AlterWireSchemaOperation::DropField { field } => {
                let index = self.field_index(field)?;
                if self.fields.len() == 1 {
                    return Err(AlterWireSchemaError::CannotDropLastField);
                }
                self.fields.remove(index);
            }
            AlterWireSchemaOperation::RenameField { field, to } => {
                let index = self.field_index(field)?;
                self.ensure_rename_target_absent(field, to)?;
                self.fields[index].name = to.clone();
            }
            AlterWireSchemaOperation::SetFieldType { field, ty } => {
                let index = self.field_index(field)?;
                self.fields[index].ty = ty.clone();
            }
            AlterWireSchemaOperation::SetFieldOptional { field, optional } => {
                let index = self.field_index(field)?;
                self.fields[index].optional = *optional;
            }
        }
        Ok(())
    }

    fn field_index(&self, field: &FieldName) -> Result<usize, AlterWireSchemaError> {
        self.fields
            .iter()
            .position(|candidate| candidate.name == *field)
            .ok_or_else(|| AlterWireSchemaError::FieldNotFound {
                field: field.clone(),
            })
    }

    fn ensure_field_absent(&self, field: &FieldName) -> Result<(), AlterWireSchemaError> {
        if self.fields.iter().any(|candidate| candidate.name == *field) {
            return Err(AlterWireSchemaError::FieldAlreadyExists {
                field: field.clone(),
            });
        }
        Ok(())
    }

    fn ensure_rename_target_absent(
        &self,
        source: &FieldName,
        target: &FieldName,
    ) -> Result<(), AlterWireSchemaError> {
        if source != target
            && self
                .fields
                .iter()
                .any(|candidate| candidate.name == *target)
        {
            return Err(AlterWireSchemaError::RenameTargetAlreadyExists {
                field: target.clone(),
            });
        }
        Ok(())
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    AsRefStr,
    Default,
)]
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

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    AsRefStr,
)]
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

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    AsRefStr,
)]
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
        len: NonZeroU32,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn field(raw: &str) -> FieldName {
        FieldName::try_from(raw).expect("valid field name")
    }

    fn schema_name(raw: &str) -> SchemaName {
        SchemaName::try_from(raw).expect("valid schema name")
    }

    fn wire_schema(raw: &str) -> WireSchemaName {
        WireSchemaName::try_from(raw).expect("valid wire schema name")
    }

    #[test]
    fn applies_internal_schema_operations_in_written_order() {
        let mut schema = CreateSchema {
            name: schema_name("events"),
            fields: vec![
                SchemaField {
                    name: field("id"),
                    ty: ParseAsType::U64,
                    optional: false,
                    sensitive: false,
                },
                SchemaField {
                    name: field("legacy"),
                    ty: ParseAsType::String,
                    optional: true,
                    sensitive: false,
                },
            ],
        };
        let alter = AlterSchema {
            schema: schema_name("events"),
            operations: vec![
                AlterSchemaOperation::AddField {
                    field: SchemaField {
                        name: field("note"),
                        ty: ParseAsType::String,
                        optional: true,
                        sensitive: false,
                    },
                },
                AlterSchemaOperation::SetFieldSensitive {
                    field: field("note"),
                    sensitive: true,
                },
                AlterSchemaOperation::RenameField {
                    field: field("id"),
                    to: field("event_id"),
                },
                AlterSchemaOperation::SetFieldType {
                    field: field("event_id"),
                    ty: ParseAsType::I64,
                },
                AlterSchemaOperation::SetFieldOptional {
                    field: field("event_id"),
                    optional: true,
                },
                AlterSchemaOperation::DropField {
                    field: field("legacy"),
                },
            ],
        };

        schema.apply_alter(&alter).expect("alter should apply");

        assert_eq!(
            schema.fields,
            vec![
                SchemaField {
                    name: field("event_id"),
                    ty: ParseAsType::I64,
                    optional: true,
                    sensitive: false,
                },
                SchemaField {
                    name: field("note"),
                    ty: ParseAsType::String,
                    optional: true,
                    sensitive: true,
                },
            ]
        );
    }

    #[test]
    fn rejects_invalid_internal_schema_operations_without_partial_application() {
        let original = CreateSchema {
            name: schema_name("events"),
            fields: vec![SchemaField {
                name: field("id"),
                ty: ParseAsType::U64,
                optional: false,
                sensitive: false,
            }],
        };
        let cases = [
            (
                AlterSchemaOperation::AddField {
                    field: original.fields[0].clone(),
                },
                AlterSchemaError::FieldAlreadyExists { field: field("id") },
            ),
            (
                AlterSchemaOperation::DropField {
                    field: field("missing"),
                },
                AlterSchemaError::FieldNotFound {
                    field: field("missing"),
                },
            ),
            (
                AlterSchemaOperation::RenameField {
                    field: field("missing"),
                    to: field("new_id"),
                },
                AlterSchemaError::FieldNotFound {
                    field: field("missing"),
                },
            ),
            (
                AlterSchemaOperation::SetFieldType {
                    field: field("missing"),
                    ty: ParseAsType::String,
                },
                AlterSchemaError::FieldNotFound {
                    field: field("missing"),
                },
            ),
            (
                AlterSchemaOperation::SetFieldOptional {
                    field: field("missing"),
                    optional: true,
                },
                AlterSchemaError::FieldNotFound {
                    field: field("missing"),
                },
            ),
            (
                AlterSchemaOperation::SetFieldSensitive {
                    field: field("missing"),
                    sensitive: true,
                },
                AlterSchemaError::FieldNotFound {
                    field: field("missing"),
                },
            ),
        ];

        for (operation, expected) in cases {
            let mut schema = original.clone();
            let error = schema
                .apply_alter(&AlterSchema {
                    schema: schema_name("events"),
                    operations: vec![operation],
                })
                .expect_err("alter should fail");
            assert_eq!(error, expected);
            assert_eq!(schema, original);
        }
    }

    #[test]
    fn rejects_dropping_the_last_internal_schema_field() {
        let mut schema = CreateSchema {
            name: schema_name("events"),
            fields: vec![SchemaField {
                name: field("id"),
                ty: ParseAsType::U64,
                optional: false,
                sensitive: false,
            }],
        };

        let error = schema
            .apply_alter(&AlterSchema {
                schema: schema_name("events"),
                operations: vec![AlterSchemaOperation::DropField { field: field("id") }],
            })
            .expect_err("alter should fail");

        assert_eq!(error, AlterSchemaError::CannotDropLastField);
        assert_eq!(schema.fields.len(), 1);
    }

    #[test]
    fn applies_exact_wire_schema_operations() {
        let mut schema = CreateWireSchema {
            name: wire_schema("payload"),
            strictness: WireSchemaStrictness::Strict,
            fields: vec![WireSchemaField {
                name: field("id"),
                ty: JsonType::Integer,
                optional: false,
            }],
        };
        let alter = AlterWireSchema {
            schema: wire_schema("payload"),
            operations: vec![
                AlterWireSchemaOperation::SetMode {
                    mode: WireSchemaStrictness::Loose,
                },
                AlterWireSchemaOperation::AddField {
                    field: WireSchemaField {
                        name: field("note"),
                        ty: JsonType::String,
                        optional: false,
                    },
                },
                AlterWireSchemaOperation::RenameField {
                    field: field("note"),
                    to: field("message"),
                },
                AlterWireSchemaOperation::SetFieldType {
                    field: field("message"),
                    ty: JsonType::Object,
                },
                AlterWireSchemaOperation::SetFieldOptional {
                    field: field("message"),
                    optional: true,
                },
            ],
        };

        schema.apply_alter(&alter).expect("alter should apply");
        assert_eq!(schema.strictness, WireSchemaStrictness::Loose);
        assert_eq!(
            schema.fields[1],
            WireSchemaField {
                name: field("message"),
                ty: JsonType::Object,
                optional: true,
            }
        );
    }
}
