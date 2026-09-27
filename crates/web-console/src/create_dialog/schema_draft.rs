//! Browser drafts for schema, wire schema, and branch creation.
//!
//! Layer: edges.
//!
//! - **Owns.** Incomplete editor state and its one-way conversion into current semantic Models.
//! - **Depends on.** Vocabulary Models, names, and the model-owned scalar and wire type catalogs.
//! - **Must not know.** Registry state, session transport, or canonical NSPL parsing.

use std::{
    collections::BTreeSet,
    num::{NonZeroU32, NonZeroU64},
};

use nervix_models::{
    AvroType, BranchEviction, BranchName, CborType, CreateBranch, CreateSchema, CreateWireSchema,
    FieldName, JsonType, Model, ParseAsType, SchemaField, SchemaName, WireSchemaField,
    WireSchemaName, WireSchemaStrictness,
};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum WireFormat {
    Json,
    Cbor,
    Avro,
}

impl WireFormat {
    pub(super) fn label(self) -> &'static str {
        match self {
            Self::Json => "JSON",
            Self::Cbor => "CBOR",
            Self::Avro => "AVRO",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct SchemaDraft {
    pub(super) name: String,
    pub(super) if_not_exists: bool,
    pub(super) fields: Vec<SchemaFieldDraft>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct SchemaFieldDraft {
    pub(super) name: String,
    pub(super) ty: SchemaTypeDraft,
    pub(super) optional: bool,
    pub(super) sensitive: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct SchemaTypeDraft {
    pub(super) scalar: Option<ParseAsType>,
    /// Collection layers in inside-to-outside order. Adding a layer wraps the current type.
    pub(super) layers: Vec<CollectionLayer>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum CollectionLayer {
    Vector,
    Array { length: String },
}

impl SchemaTypeDraft {
    fn build(&self, field: usize) -> Result<ParseAsType, SchemaDraftError> {
        let Some(mut ty) = self.scalar.clone() else {
            return Err(SchemaDraftError::FieldTypeRequired { field });
        };
        for (position, layer) in self.layers.iter().enumerate() {
            ty = match layer {
                CollectionLayer::Vector => ParseAsType::Vec {
                    element: Box::new(ty),
                },
                CollectionLayer::Array { length } => {
                    let parsed = length.trim().parse::<NonZeroU32>().map_err(|_| {
                        SchemaDraftError::ArrayLength {
                            field,
                            layer: position + 1,
                        }
                    })?;
                    if parsed.get() > i32::MAX.unsigned_abs() {
                        return Err(SchemaDraftError::ArrayLength {
                            field,
                            layer: position + 1,
                        });
                    }
                    ParseAsType::Array {
                        element: Box::new(ty),
                        len: parsed,
                    }
                }
            };
        }
        Ok(ty)
    }
}

impl SchemaDraft {
    pub(super) fn build(&self) -> Result<CreateSchema, SchemaDraftError> {
        let name = SchemaName::parse(self.name.trim()).map_err(|_| SchemaDraftError::SchemaName)?;
        if self.fields.is_empty() {
            return Err(SchemaDraftError::FieldsRequired);
        }
        let mut names = BTreeSet::new();
        let mut fields = Vec::with_capacity(self.fields.len());
        for (index, draft) in self.fields.iter().enumerate() {
            let field = index + 1;
            let name = FieldName::parse(draft.name.trim())
                .map_err(|_| SchemaDraftError::FieldName { field })?;
            if !names.insert(name.clone()) {
                return Err(SchemaDraftError::DuplicateField { name });
            }
            fields.push(SchemaField {
                name,
                ty: draft.ty.build(field)?,
                optional: draft.optional,
                sensitive: draft.sensitive,
            });
        }
        Ok(CreateSchema { name, fields })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WireFieldType {
    Json(JsonType),
    Avro(AvroType),
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct WireFieldDraft {
    pub(super) name: String,
    pub(super) ty: Option<WireFieldType>,
    pub(super) optional: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct WireSchemaDraft {
    pub(super) name: String,
    pub(super) if_not_exists: bool,
    pub(super) mode: Option<WireSchemaStrictness>,
    pub(super) fields: Vec<WireFieldDraft>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct StructuredDrafts {
    pub(super) schema: SchemaDraft,
    pub(super) wire_json: WireSchemaDraft,
    pub(super) wire_cbor: WireSchemaDraft,
    pub(super) wire_avro: WireSchemaDraft,
    pub(super) branch: BranchDraft,
}

impl StructuredDrafts {
    pub(super) fn wire(&self, format: WireFormat) -> &WireSchemaDraft {
        match format {
            WireFormat::Json => &self.wire_json,
            WireFormat::Cbor => &self.wire_cbor,
            WireFormat::Avro => &self.wire_avro,
        }
    }

    pub(super) fn wire_mut(&mut self, format: WireFormat) -> &mut WireSchemaDraft {
        match format {
            WireFormat::Json => &mut self.wire_json,
            WireFormat::Cbor => &mut self.wire_cbor,
            WireFormat::Avro => &mut self.wire_avro,
        }
    }
}

impl WireSchemaDraft {
    pub(super) fn build(&self, format: WireFormat) -> Result<Model, SchemaDraftError> {
        let name = WireSchemaName::parse(self.name.trim())
            .map_err(|_| SchemaDraftError::WireSchemaName)?;
        let Some(strictness) = self.mode else {
            return Err(SchemaDraftError::WireModeRequired);
        };
        if self.fields.is_empty() {
            return Err(SchemaDraftError::FieldsRequired);
        }
        let mut names = BTreeSet::new();
        match format {
            WireFormat::Json | WireFormat::Cbor => {
                let mut fields = Vec::<WireSchemaField<CborType>>::with_capacity(self.fields.len());
                for (index, draft) in self.fields.iter().enumerate() {
                    let field = index + 1;
                    let field_name = FieldName::parse(draft.name.trim())
                        .map_err(|_| SchemaDraftError::FieldName { field })?;
                    if !names.insert(field_name.clone()) {
                        return Err(SchemaDraftError::DuplicateField { name: field_name });
                    }
                    let Some(WireFieldType::Json(ty)) = draft.ty else {
                        return Err(SchemaDraftError::FieldTypeRequired { field });
                    };
                    fields.push(WireSchemaField {
                        name: field_name,
                        ty,
                        optional: draft.optional,
                    });
                }
                let schema = CreateWireSchema {
                    name,
                    strictness,
                    fields,
                };
                if format == WireFormat::Json {
                    Ok(Model::WireJsonSchema(schema))
                } else {
                    Ok(Model::WireCborSchema(schema))
                }
            }
            WireFormat::Avro => {
                let mut fields = Vec::with_capacity(self.fields.len());
                for (index, draft) in self.fields.iter().enumerate() {
                    let field = index + 1;
                    let field_name = FieldName::parse(draft.name.trim())
                        .map_err(|_| SchemaDraftError::FieldName { field })?;
                    if !names.insert(field_name.clone()) {
                        return Err(SchemaDraftError::DuplicateField { name: field_name });
                    }
                    let Some(WireFieldType::Avro(ty)) = draft.ty else {
                        return Err(SchemaDraftError::FieldTypeRequired { field });
                    };
                    fields.push(WireSchemaField {
                        name: field_name,
                        ty,
                        optional: draft.optional,
                    });
                }
                Ok(Model::WireAvroSchema(CreateWireSchema {
                    name,
                    strictness,
                    fields,
                }))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct BranchDraft {
    pub(super) name: String,
    pub(super) if_not_exists: bool,
    pub(super) schema: Option<SchemaName>,
    /// A changed domain keeps the prior reference visible until it is reselected.
    pub(super) schema_valid: bool,
    pub(super) ttl: String,
    pub(super) limit_instances: bool,
    pub(super) max_instances: String,
}

impl BranchDraft {
    pub(super) fn build(&self) -> Result<CreateBranch, SchemaDraftError> {
        let name = BranchName::parse(self.name.trim()).map_err(|_| SchemaDraftError::BranchName)?;
        let Some(schema) = &self.schema else {
            return Err(SchemaDraftError::SchemaReferenceRequired);
        };
        if !self.schema_valid {
            return Err(SchemaDraftError::SchemaReferenceChanged);
        }
        let ttl = self.ttl.trim();
        if ttl.is_empty() {
            return Err(SchemaDraftError::TtlRequired);
        }
        let eviction = if self.limit_instances {
            let max_instances = self
                .max_instances
                .trim()
                .parse::<NonZeroU64>()
                .map_err(|_| SchemaDraftError::MaxInstances)?;
            Some(BranchEviction::Lru { max_instances })
        } else {
            None
        };
        Ok(CreateBranch {
            name,
            schema: schema.clone(),
            ttl: ttl.to_string(),
            eviction,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(super) enum SchemaDraftError {
    #[error("Schema name is invalid")]
    SchemaName,
    #[error("Wire schema name is invalid")]
    WireSchemaName,
    #[error("Branch name is invalid")]
    BranchName,
    #[error("Add at least one field")]
    FieldsRequired,
    #[error("Field {field} has an invalid name")]
    FieldName { field: usize },
    #[error("Field {field} needs a type")]
    FieldTypeRequired { field: usize },
    #[error("Field '{name}' is declared more than once")]
    DuplicateField { name: FieldName },
    #[error("Field {field} has an invalid fixed-array length at layer {layer}")]
    ArrayLength { field: usize, layer: usize },
    #[error("Choose STRICT or LOOSE mode")]
    WireModeRequired,
    #[error("Choose a schema for the branch")]
    SchemaReferenceRequired,
    #[error("The selected schema belongs to a changed context; select it again")]
    SchemaReferenceChanged,
    #[error("Branch TTL is required")]
    TtlRequired,
    #[error("Maximum instances must be a positive integer")]
    MaxInstances,
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_models::{
        AvroType, CreateStatement, JsonType, Model, ParseAsType, RequestedResourceVersion,
        SchemaName, Statement, WireSchemaStrictness,
    };
    use nervix_nspl::client_statement::{ClientStatement, parse_client_statement};
    use strum::IntoEnumIterator;

    use super::*;

    fn assert_canonical_round_trip(model: Model, if_not_exists: bool) {
        let model: Model<RequestedResourceVersion> = model.into();
        let statement = Statement::Create(CreateStatement::new(Box::new(model), if_not_exists));
        let source = statement
            .to_canonical_nspl()
            .assured("every completed current Model has a canonical command");
        let parsed = parse_client_statement(&source)
            .assured("the canonical command must parse back to the same Model");
        assert_eq!(parsed, ClientStatement::Server(statement));
    }

    #[test]
    fn all_scalar_and_nested_types_keep_order_and_exact_modifiers() {
        let mut fields = Vec::new();
        for (index, ty) in ParseAsType::scalar_variants().iter().enumerate() {
            fields.push(SchemaFieldDraft {
                name: format!("field_{index}"),
                ty: SchemaTypeDraft {
                    scalar: Some(ty.clone()),
                    layers: Vec::new(),
                },
                optional: index % 2 == 0,
                sensitive: index % 3 == 0,
            });
        }
        fields.push(SchemaFieldDraft {
            name: "nested".to_string(),
            ty: SchemaTypeDraft {
                scalar: Some(ParseAsType::F32),
                layers: vec![
                    CollectionLayer::Array {
                        length: "6".to_string(),
                    },
                    CollectionLayer::Vector,
                    CollectionLayer::Array {
                        length: "2".to_string(),
                    },
                ],
            },
            optional: true,
            sensitive: true,
        });
        let built = SchemaDraft {
            name: "complete".to_string(),
            if_not_exists: false,
            fields,
        }
        .build()
        .assured("every test field has a distinct valid name and a complete type");
        assert_eq!(built.fields.len(), ParseAsType::scalar_variants().len() + 1);
        assert_eq!(built.fields[0].name.as_str(), "field_0");
        assert!(built.fields[0].optional && built.fields[0].sensitive);
        assert_eq!(
            built
                .fields
                .last()
                .assured("the nested field was appended")
                .ty,
            ParseAsType::Array {
                element: Box::new(ParseAsType::Vec {
                    element: Box::new(ParseAsType::Array {
                        element: Box::new(ParseAsType::F32),
                        len: NonZeroU32::new(6).assured("six is nonzero"),
                    }),
                }),
                len: NonZeroU32::new(2).assured("two is nonzero"),
            }
        );
        assert_canonical_round_trip(Model::Schema(built), true);
    }

    #[test]
    fn each_wire_format_builds_its_own_model_with_every_type() {
        for format in [WireFormat::Json, WireFormat::Cbor, WireFormat::Avro] {
            let types = match format {
                WireFormat::Json | WireFormat::Cbor => JsonType::iter()
                    .map(WireFieldType::Json)
                    .collect::<Vec<_>>(),
                WireFormat::Avro => AvroType::iter()
                    .map(WireFieldType::Avro)
                    .collect::<Vec<_>>(),
            };
            let fields = types
                .into_iter()
                .enumerate()
                .map(|(index, ty)| WireFieldDraft {
                    name: format!("field_{index}"),
                    ty: Some(ty),
                    optional: index % 2 == 0,
                })
                .collect();
            let draft = WireSchemaDraft {
                name: "shared".to_string(),
                if_not_exists: false,
                mode: Some(WireSchemaStrictness::Loose),
                fields,
            };
            let model = draft
                .build(format)
                .assured("the wire types belong to their selected format");
            assert_canonical_round_trip(model.clone(), true);
            match (format, model) {
                (WireFormat::Json, Model::WireJsonSchema(schema)) => {
                    assert_eq!(schema.fields.len(), JsonType::iter().count())
                }
                (WireFormat::Cbor, Model::WireCborSchema(schema)) => {
                    assert_eq!(schema.fields.len(), JsonType::iter().count())
                }
                (WireFormat::Avro, Model::WireAvroSchema(schema)) => {
                    assert_eq!(schema.fields.len(), AvroType::iter().count())
                }
                _ => panic!("each format must keep its model identity"),
            }
        }
    }

    #[test]
    fn branch_limit_and_invalid_drafts_keep_exact_required_values() {
        let schema = SchemaName::parse("branch_key").assured("the test name is valid");
        let mut branch = BranchDraft {
            name: "by_key".to_string(),
            schema: Some(schema.clone()),
            schema_valid: false,
            ttl: "5m".to_string(),
            limit_instances: true,
            max_instances: "3".to_string(),
            ..BranchDraft::default()
        };
        assert_eq!(
            branch.build(),
            Err(SchemaDraftError::SchemaReferenceChanged)
        );
        branch.schema_valid = true;
        let model = branch
            .build()
            .assured("the selected schema and positive limit are valid");
        assert_eq!(model.schema, schema);
        assert_eq!(
            model
                .eviction
                .as_ref()
                .map(BranchEviction::max_instances)
                .map(NonZeroU64::get),
            Some(3)
        );
        assert_canonical_round_trip(Model::Branch(model), false);
        branch.max_instances = "0".to_string();
        assert_eq!(branch.build(), Err(SchemaDraftError::MaxInstances));
    }

    #[test]
    fn missing_types_modes_and_duplicate_names_do_not_become_models() {
        let mut schema = SchemaDraft {
            name: "record".to_string(),
            fields: vec![SchemaFieldDraft {
                name: "same".to_string(),
                ty: SchemaTypeDraft {
                    scalar: Some(ParseAsType::U32),
                    layers: Vec::new(),
                },
                ..SchemaFieldDraft::default()
            }],
            ..SchemaDraft::default()
        };
        schema.fields.push(schema.fields[0].clone());
        assert_eq!(
            schema.build(),
            Err(SchemaDraftError::DuplicateField {
                name: FieldName::parse("same").assured("the test field name is valid"),
            })
        );
        schema.fields.pop();
        schema.fields[0].ty.scalar = None;
        assert_eq!(
            schema.build(),
            Err(SchemaDraftError::FieldTypeRequired { field: 1 })
        );

        let wire = WireSchemaDraft {
            name: "wire".to_string(),
            fields: vec![WireFieldDraft {
                name: "value".to_string(),
                ty: Some(WireFieldType::Json(JsonType::String)),
                optional: false,
            }],
            ..WireSchemaDraft::default()
        };
        assert_eq!(
            wire.build(WireFormat::Json),
            Err(SchemaDraftError::WireModeRequired)
        );
    }

    #[test]
    fn incomplete_collection_wire_and_branch_choices_remain_explicit_errors() {
        let mut collection = SchemaTypeDraft {
            scalar: Some(ParseAsType::U32),
            layers: vec![CollectionLayer::Array {
                length: "0".to_string(),
            }],
        };
        assert_eq!(
            collection.build(1),
            Err(SchemaDraftError::ArrayLength { field: 1, layer: 1 })
        );
        collection.layers[0] = CollectionLayer::Array {
            length: "2147483648".to_string(),
        };
        assert_eq!(
            collection.build(1),
            Err(SchemaDraftError::ArrayLength { field: 1, layer: 1 })
        );

        let schema = SchemaDraft {
            name: "record".to_string(),
            ..SchemaDraft::default()
        };
        assert_eq!(schema.build(), Err(SchemaDraftError::FieldsRequired));

        let mut wire = WireSchemaDraft {
            name: "wire".to_string(),
            mode: Some(WireSchemaStrictness::Strict),
            ..WireSchemaDraft::default()
        };
        assert_eq!(
            wire.build(WireFormat::Json),
            Err(SchemaDraftError::FieldsRequired)
        );
        wire.fields.push(WireFieldDraft {
            name: "payload".to_string(),
            ty: Some(WireFieldType::Json(JsonType::String)),
            optional: false,
        });
        assert_eq!(
            wire.build(WireFormat::Avro),
            Err(SchemaDraftError::FieldTypeRequired { field: 1 })
        );
        wire.fields.push(wire.fields[0].clone());
        assert_eq!(
            wire.build(WireFormat::Json),
            Err(SchemaDraftError::DuplicateField {
                name: FieldName::parse("payload").assured("valid field name"),
            })
        );

        let mut branch = BranchDraft {
            name: "by_tenant".to_string(),
            ..BranchDraft::default()
        };
        assert_eq!(
            branch.build(),
            Err(SchemaDraftError::SchemaReferenceRequired)
        );
        branch.schema = Some(SchemaName::parse("tenant_key").assured("valid schema name"));
        branch.schema_valid = true;
        assert_eq!(branch.build(), Err(SchemaDraftError::TtlRequired));
    }
}
