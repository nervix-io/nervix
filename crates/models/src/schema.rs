use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use strum::AsRefStr;
use thiserror::Error;

use crate::Identifier;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WireSchemaDefinition {
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
pub struct AlterSchema {
    pub schema: Identifier,
    pub operations: Vec<AlterSchemaOperation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AlterSchemaOperation {
    AddField { field: SchemaField },
    DropField { field: Identifier },
    RenameField { field: Identifier, to: Identifier },
    SetFieldType { field: Identifier, ty: ParseAsType },
    SetFieldOptional { field: Identifier, optional: bool },
    SetFieldSensitive { field: Identifier, sensitive: bool },
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
pub struct AlterWireSchema<T> {
    pub schema: Identifier,
    pub operations: Vec<AlterWireSchemaOperation<T>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AlterWireSchemaOperation<T> {
    SetMode { mode: WireSchemaStrictness },
    AddField { field: WireSchemaField<T> },
    DropField { field: Identifier },
    RenameField { field: Identifier, to: Identifier },
    SetFieldType { field: Identifier, ty: T },
    SetFieldOptional { field: Identifier, optional: bool },
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

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AlterSchemaError {
    #[error("ALTER targets schema `{requested}`, but the stored schema is `{stored}`")]
    SchemaNameMismatch {
        stored: Identifier,
        requested: Identifier,
    },
    #[error("field `{field}` already exists")]
    FieldAlreadyExists { field: Identifier },
    #[error("field `{field}` does not exist")]
    FieldNotFound { field: Identifier },
    #[error("cannot rename field to `{field}` because that field already exists")]
    RenameTargetAlreadyExists { field: Identifier },
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

    fn field_index(&self, field: &Identifier) -> Result<usize, AlterSchemaError> {
        self.fields
            .iter()
            .position(|candidate| candidate.name == *field)
            .ok_or_else(|| AlterSchemaError::FieldNotFound {
                field: field.clone(),
            })
    }

    fn ensure_field_absent(&self, field: &Identifier) -> Result<(), AlterSchemaError> {
        if self.fields.iter().any(|candidate| candidate.name == *field) {
            return Err(AlterSchemaError::FieldAlreadyExists {
                field: field.clone(),
            });
        }
        Ok(())
    }

    fn ensure_rename_target_absent(
        &self,
        source: &Identifier,
        target: &Identifier,
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
    pub fn apply_alter(&mut self, alter: &AlterWireSchema<T>) -> Result<(), AlterSchemaError> {
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
        operation: &AlterWireSchemaOperation<T>,
    ) -> Result<(), AlterSchemaError> {
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
                    return Err(AlterSchemaError::CannotDropLastField);
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

    fn field_index(&self, field: &Identifier) -> Result<usize, AlterSchemaError> {
        self.fields
            .iter()
            .position(|candidate| candidate.name == *field)
            .ok_or_else(|| AlterSchemaError::FieldNotFound {
                field: field.clone(),
            })
    }

    fn ensure_field_absent(&self, field: &Identifier) -> Result<(), AlterSchemaError> {
        if self.fields.iter().any(|candidate| candidate.name == *field) {
            return Err(AlterSchemaError::FieldAlreadyExists {
                field: field.clone(),
            });
        }
        Ok(())
    }

    fn ensure_rename_target_absent(
        &self,
        source: &Identifier,
        target: &Identifier,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn identifier(raw: &str) -> Identifier {
        Identifier::try_from(raw).expect("valid identifier")
    }

    #[test]
    fn applies_internal_schema_operations_in_written_order() {
        let mut schema = CreateSchema {
            name: identifier("events"),
            fields: vec![
                SchemaField {
                    name: identifier("id"),
                    ty: ParseAsType::U64,
                    optional: false,
                    sensitive: false,
                },
                SchemaField {
                    name: identifier("legacy"),
                    ty: ParseAsType::String,
                    optional: true,
                    sensitive: false,
                },
            ],
        };
        let alter = AlterSchema {
            schema: identifier("events"),
            operations: vec![
                AlterSchemaOperation::AddField {
                    field: SchemaField {
                        name: identifier("note"),
                        ty: ParseAsType::String,
                        optional: true,
                        sensitive: false,
                    },
                },
                AlterSchemaOperation::SetFieldSensitive {
                    field: identifier("note"),
                    sensitive: true,
                },
                AlterSchemaOperation::RenameField {
                    field: identifier("id"),
                    to: identifier("event_id"),
                },
                AlterSchemaOperation::SetFieldType {
                    field: identifier("event_id"),
                    ty: ParseAsType::I64,
                },
                AlterSchemaOperation::SetFieldOptional {
                    field: identifier("event_id"),
                    optional: true,
                },
                AlterSchemaOperation::DropField {
                    field: identifier("legacy"),
                },
            ],
        };

        schema.apply_alter(&alter).expect("alter should apply");

        assert_eq!(
            schema.fields,
            vec![
                SchemaField {
                    name: identifier("event_id"),
                    ty: ParseAsType::I64,
                    optional: true,
                    sensitive: false,
                },
                SchemaField {
                    name: identifier("note"),
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
            name: identifier("events"),
            fields: vec![SchemaField {
                name: identifier("id"),
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
                AlterSchemaError::FieldAlreadyExists {
                    field: identifier("id"),
                },
            ),
            (
                AlterSchemaOperation::DropField {
                    field: identifier("missing"),
                },
                AlterSchemaError::FieldNotFound {
                    field: identifier("missing"),
                },
            ),
            (
                AlterSchemaOperation::RenameField {
                    field: identifier("missing"),
                    to: identifier("new_id"),
                },
                AlterSchemaError::FieldNotFound {
                    field: identifier("missing"),
                },
            ),
            (
                AlterSchemaOperation::SetFieldType {
                    field: identifier("missing"),
                    ty: ParseAsType::String,
                },
                AlterSchemaError::FieldNotFound {
                    field: identifier("missing"),
                },
            ),
            (
                AlterSchemaOperation::SetFieldOptional {
                    field: identifier("missing"),
                    optional: true,
                },
                AlterSchemaError::FieldNotFound {
                    field: identifier("missing"),
                },
            ),
            (
                AlterSchemaOperation::SetFieldSensitive {
                    field: identifier("missing"),
                    sensitive: true,
                },
                AlterSchemaError::FieldNotFound {
                    field: identifier("missing"),
                },
            ),
        ];

        for (operation, expected) in cases {
            let mut schema = original.clone();
            let error = schema
                .apply_alter(&AlterSchema {
                    schema: identifier("events"),
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
            name: identifier("events"),
            fields: vec![SchemaField {
                name: identifier("id"),
                ty: ParseAsType::U64,
                optional: false,
                sensitive: false,
            }],
        };

        let error = schema
            .apply_alter(&AlterSchema {
                schema: identifier("events"),
                operations: vec![AlterSchemaOperation::DropField {
                    field: identifier("id"),
                }],
            })
            .expect_err("alter should fail");

        assert_eq!(error, AlterSchemaError::CannotDropLastField);
        assert_eq!(schema.fields.len(), 1);
    }

    #[test]
    fn applies_exact_wire_schema_operations() {
        let mut schema = CreateWireSchema {
            name: identifier("payload"),
            strictness: WireSchemaStrictness::Strict,
            fields: vec![WireSchemaField {
                name: identifier("id"),
                ty: JsonType::Integer,
                optional: false,
            }],
        };
        let alter = AlterWireSchema {
            schema: identifier("payload"),
            operations: vec![
                AlterWireSchemaOperation::SetMode {
                    mode: WireSchemaStrictness::Loose,
                },
                AlterWireSchemaOperation::AddField {
                    field: WireSchemaField {
                        name: identifier("note"),
                        ty: JsonType::String,
                        optional: false,
                    },
                },
                AlterWireSchemaOperation::RenameField {
                    field: identifier("note"),
                    to: identifier("message"),
                },
                AlterWireSchemaOperation::SetFieldType {
                    field: identifier("message"),
                    ty: JsonType::Object,
                },
                AlterWireSchemaOperation::SetFieldOptional {
                    field: identifier("message"),
                    optional: true,
                },
            ],
        };

        schema.apply_alter(&alter).expect("alter should apply");
        assert_eq!(schema.strictness, WireSchemaStrictness::Loose);
        assert_eq!(
            schema.fields[1],
            WireSchemaField {
                name: identifier("message"),
                ty: JsonType::Object,
                optional: true,
            }
        );
    }
}
