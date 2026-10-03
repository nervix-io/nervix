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

use error_stack::Report;
use nervix_models::{
    AvroType, BranchEviction, BranchName, CborType, CreateBranch, CreateSchema, CreateWireSchema,
    FieldName, JsonType, Model, ModelKind, NodeRef, ParseAsType, SchemaField, SchemaName,
    WireSchemaField, WireSchemaName, WireSchemaStrictness,
};
use thiserror::Error;

use super::SelectedReference;

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
    pub(super) fn build(&self, field: usize) -> error_stack::Result<ParseAsType, SchemaDraftError> {
        let Some(mut ty) = self.scalar.clone() else {
            return Err(Report::new(SchemaDraftError::FieldTypeRequired { field }));
        };
        for (position, layer) in self.layers.iter().enumerate() {
            ty = match layer {
                CollectionLayer::Vector => ParseAsType::Vec {
                    element: Box::new(ty),
                },
                CollectionLayer::Array { length } => {
                    let parsed = length.trim().parse::<NonZeroU32>().map_err(|_| {
                        Report::new(SchemaDraftError::ArrayLength {
                            field,
                            layer: position + 1,
                        })
                    })?;
                    if parsed.get() > i32::MAX.unsigned_abs() {
                        return Err(Report::new(SchemaDraftError::ArrayLength {
                            field,
                            layer: position + 1,
                        }));
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
    pub(super) fn move_field_up(&mut self, index: usize) {
        move_item_up(&mut self.fields, index);
    }

    pub(super) fn move_field_down(&mut self, index: usize) {
        move_item_down(&mut self.fields, index);
    }

    pub(super) fn remove_field(&mut self, index: usize) {
        remove_item(&mut self.fields, index);
    }

    pub(super) fn set_field_name(&mut self, index: usize, name: String) {
        if let Some(field) = self.fields.get_mut(index) {
            field.name = name;
        }
    }

    pub(super) fn set_field_scalar(&mut self, index: usize, ty: ParseAsType) {
        if let Some(field) = self.fields.get_mut(index) {
            field.ty.scalar = Some(ty);
        }
    }

    pub(super) fn push_field_layer(&mut self, index: usize, layer: CollectionLayer) {
        if let Some(field) = self.fields.get_mut(index) {
            field.ty.layers.push(layer);
        }
    }

    pub(super) fn set_array_length(&mut self, index: usize, layer: usize, value: String) {
        if let Some(field) = self.fields.get_mut(index)
            && let Some(CollectionLayer::Array { length }) = field.ty.layers.get_mut(layer)
        {
            *length = value;
        }
    }

    pub(super) fn remove_field_layer(&mut self, index: usize, layer: usize) {
        if let Some(field) = self.fields.get_mut(index) {
            remove_item(&mut field.ty.layers, layer);
        }
    }

    pub(super) fn set_field_optional(&mut self, index: usize, optional: bool) {
        if let Some(field) = self.fields.get_mut(index) {
            field.optional = optional;
        }
    }

    pub(super) fn set_field_sensitive(&mut self, index: usize, sensitive: bool) {
        if let Some(field) = self.fields.get_mut(index) {
            field.sensitive = sensitive;
        }
    }

    pub(super) fn build(&self) -> error_stack::Result<CreateSchema, SchemaDraftError> {
        let name = SchemaName::parse(self.name.trim())
            .map_err(|_| Report::new(SchemaDraftError::SchemaName))?;
        if self.fields.is_empty() {
            return Err(Report::new(SchemaDraftError::FieldsRequired));
        }
        let mut names = BTreeSet::new();
        let mut fields = Vec::with_capacity(self.fields.len());
        for (index, draft) in self.fields.iter().enumerate() {
            let field = index + 1;
            let name = FieldName::parse(draft.name.trim())
                .map_err(|_| Report::new(SchemaDraftError::FieldName { field }))?;
            if !names.insert(name.clone()) {
                return Err(Report::new(SchemaDraftError::DuplicateField { name }));
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
    pub(super) fn move_field_up(&mut self, index: usize) {
        move_item_up(&mut self.fields, index);
    }

    pub(super) fn move_field_down(&mut self, index: usize) {
        move_item_down(&mut self.fields, index);
    }

    pub(super) fn remove_field(&mut self, index: usize) {
        remove_item(&mut self.fields, index);
    }

    pub(super) fn set_field_name(&mut self, index: usize, name: String) {
        if let Some(field) = self.fields.get_mut(index) {
            field.name = name;
        }
    }

    pub(super) fn set_field_type(&mut self, index: usize, ty: WireFieldType) {
        if let Some(field) = self.fields.get_mut(index) {
            field.ty = Some(ty);
        }
    }

    pub(super) fn set_field_optional(&mut self, index: usize, optional: bool) {
        if let Some(field) = self.fields.get_mut(index) {
            field.optional = optional;
        }
    }

    pub(super) fn build(&self, format: WireFormat) -> error_stack::Result<Model, SchemaDraftError> {
        let name = WireSchemaName::parse(self.name.trim())
            .map_err(|_| Report::new(SchemaDraftError::WireSchemaName))?;
        let Some(strictness) = self.mode else {
            return Err(Report::new(SchemaDraftError::WireModeRequired));
        };
        if self.fields.is_empty() {
            return Err(Report::new(SchemaDraftError::FieldsRequired));
        }
        let mut names = BTreeSet::new();
        match format {
            WireFormat::Json | WireFormat::Cbor => {
                let mut fields = Vec::<WireSchemaField<CborType>>::with_capacity(self.fields.len());
                for (index, draft) in self.fields.iter().enumerate() {
                    let field = index + 1;
                    let field_name = FieldName::parse(draft.name.trim())
                        .map_err(|_| Report::new(SchemaDraftError::FieldName { field }))?;
                    if !names.insert(field_name.clone()) {
                        return Err(Report::new(SchemaDraftError::DuplicateField {
                            name: field_name,
                        }));
                    }
                    let Some(WireFieldType::Json(ty)) = draft.ty else {
                        return Err(Report::new(SchemaDraftError::FieldTypeRequired { field }));
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
                        .map_err(|_| Report::new(SchemaDraftError::FieldName { field }))?;
                    if !names.insert(field_name.clone()) {
                        return Err(Report::new(SchemaDraftError::DuplicateField {
                            name: field_name,
                        }));
                    }
                    let Some(WireFieldType::Avro(ty)) = draft.ty else {
                        return Err(Report::new(SchemaDraftError::FieldTypeRequired { field }));
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

fn move_item_up<T>(items: &mut [T], index: usize) {
    if index > 0 && index < items.len() {
        items.swap(index, index - 1);
    }
}

fn move_item_down<T>(items: &mut [T], index: usize) {
    if let Some(next) = index.checked_add(1)
        && next < items.len()
    {
        items.swap(index, next);
    }
}

fn remove_item<T>(items: &mut Vec<T>, index: usize) {
    if index < items.len() {
        items.remove(index);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct BranchDraft {
    pub(super) name: String,
    pub(super) if_not_exists: bool,
    /// A changed domain keeps the prior reference visible until it is reselected.
    pub(super) schema: Option<SelectedReference<SchemaName>>,
    pub(super) ttl: String,
    pub(super) limit_instances: bool,
    pub(super) max_instances: String,
}

impl BranchDraft {
    pub(super) fn select_schema(&mut self, node: &NodeRef) {
        if node.kind == ModelKind::Schema {
            self.schema = Some(SelectedReference::chosen(SchemaName::from(
                &node.identifier,
            )));
        }
    }

    pub(super) fn selects_schema(&self, node: &NodeRef) -> bool {
        match &self.schema {
            Some(schema) => schema.selects(ModelKind::Schema, node),
            None => false,
        }
    }

    /// Keeps the selected key schema visible after the draft moved to another domain, but
    /// requires selecting it again.
    pub(super) fn invalidate_references(&mut self) {
        if let Some(schema) = &mut self.schema {
            schema.invalidate();
        }
    }

    pub(super) fn build(&self) -> error_stack::Result<CreateBranch, SchemaDraftError> {
        let name = BranchName::parse(self.name.trim())
            .map_err(|_| Report::new(SchemaDraftError::BranchName))?;
        let Some(schema) = &self.schema else {
            return Err(Report::new(SchemaDraftError::SchemaReferenceRequired));
        };
        let Some(schema) = schema.current_name() else {
            return Err(Report::new(SchemaDraftError::SchemaReferenceChanged));
        };
        let ttl = self.ttl.trim();
        if ttl.is_empty() {
            return Err(Report::new(SchemaDraftError::TtlRequired));
        }
        let eviction = if self.limit_instances {
            let max_instances = self
                .max_instances
                .trim()
                .parse::<NonZeroU64>()
                .map_err(|_| Report::new(SchemaDraftError::MaxInstances))?;
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

    fn draft_error<T>(result: error_stack::Result<T, SchemaDraftError>) -> SchemaDraftError {
        result
            .err()
            .verified("the draft in this assertion has a required value missing or invalid")
            .current_context()
            .clone()
    }

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
        let mut selected = SelectedReference::chosen(schema.clone());
        selected.invalidate();
        let mut branch = BranchDraft {
            name: "by_key".to_string(),
            schema: Some(selected),
            ttl: "5m".to_string(),
            limit_instances: true,
            max_instances: "3".to_string(),
            ..BranchDraft::default()
        };
        assert_eq!(
            draft_error(branch.build()),
            SchemaDraftError::SchemaReferenceChanged
        );
        branch.schema = Some(SelectedReference::chosen(schema.clone()));
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
        assert_eq!(draft_error(branch.build()), SchemaDraftError::MaxInstances);
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
            draft_error(schema.build()),
            SchemaDraftError::DuplicateField {
                name: FieldName::parse("same").assured("the test field name is valid"),
            }
        );
        schema.fields.pop();
        schema.fields[0].ty.scalar = None;
        assert_eq!(
            draft_error(schema.build()),
            SchemaDraftError::FieldTypeRequired { field: 1 }
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
            draft_error(wire.build(WireFormat::Json)),
            SchemaDraftError::WireModeRequired
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
            draft_error(collection.build(1)),
            SchemaDraftError::ArrayLength { field: 1, layer: 1 }
        );
        collection.layers[0] = CollectionLayer::Array {
            length: "2147483648".to_string(),
        };
        assert_eq!(
            draft_error(collection.build(1)),
            SchemaDraftError::ArrayLength { field: 1, layer: 1 }
        );

        let schema = SchemaDraft {
            name: "record".to_string(),
            ..SchemaDraft::default()
        };
        assert_eq!(
            draft_error(schema.build()),
            SchemaDraftError::FieldsRequired
        );

        let mut wire = WireSchemaDraft {
            name: "wire".to_string(),
            mode: Some(WireSchemaStrictness::Strict),
            ..WireSchemaDraft::default()
        };
        assert_eq!(
            draft_error(wire.build(WireFormat::Json)),
            SchemaDraftError::FieldsRequired
        );
        wire.fields.push(WireFieldDraft {
            name: "payload".to_string(),
            ty: Some(WireFieldType::Json(JsonType::String)),
            optional: false,
        });
        assert_eq!(
            draft_error(wire.build(WireFormat::Avro)),
            SchemaDraftError::FieldTypeRequired { field: 1 }
        );
        wire.fields.push(wire.fields[0].clone());
        assert_eq!(
            draft_error(wire.build(WireFormat::Json)),
            SchemaDraftError::DuplicateField {
                name: FieldName::parse("payload").assured("valid field name"),
            }
        );

        let mut branch = BranchDraft {
            name: "by_tenant".to_string(),
            ..BranchDraft::default()
        };
        assert_eq!(
            draft_error(branch.build()),
            SchemaDraftError::SchemaReferenceRequired
        );
        branch.schema = Some(SelectedReference::chosen(
            SchemaName::parse("tenant_key").assured("valid schema name"),
        ));
        assert_eq!(draft_error(branch.build()), SchemaDraftError::TtlRequired);
    }

    #[test]
    fn ordered_schema_edits_keep_nested_types_and_ignore_stale_field_callbacks() {
        let mut draft = SchemaDraft {
            name: "ordered".to_string(),
            fields: vec![SchemaFieldDraft::default(); 3],
            ..SchemaDraft::default()
        };
        for (index, name) in ["first", "second", "third"].into_iter().enumerate() {
            draft.set_field_name(index, name.to_string());
            draft.set_field_scalar(index, ParseAsType::U32);
        }
        draft.move_field_up(2);
        draft.move_field_down(0);
        draft.remove_field(2);
        draft.push_field_layer(0, CollectionLayer::Vector);
        draft.remove_field_layer(0, 0);
        draft.push_field_layer(0, CollectionLayer::Vector);
        draft.push_field_layer(
            0,
            CollectionLayer::Array {
                length: String::new(),
            },
        );
        draft.set_array_length(0, 1, "3".to_string());
        draft.set_field_optional(1, true);
        draft.set_field_sensitive(1, true);

        let snapshot = draft.clone();
        draft.move_field_up(0);
        draft.move_field_down(usize::MAX);
        draft.remove_field(usize::MAX);
        draft.set_field_name(usize::MAX, "stale".to_string());
        draft.set_field_scalar(usize::MAX, ParseAsType::String);
        draft.push_field_layer(usize::MAX, CollectionLayer::Vector);
        draft.set_array_length(0, 0, "99".to_string());
        draft.remove_field_layer(0, usize::MAX);
        draft.set_field_optional(usize::MAX, false);
        draft.set_field_sensitive(usize::MAX, false);
        assert_eq!(draft, snapshot);

        let model = draft
            .build()
            .assured("the edited fields have names and scalar types");
        assert_eq!(
            model
                .fields
                .iter()
                .map(|field| field.name.as_ref())
                .collect::<Vec<_>>(),
            ["third", "first"]
        );
        assert_eq!(
            model.fields[0].ty,
            ParseAsType::Array {
                element: Box::new(ParseAsType::Vec {
                    element: Box::new(ParseAsType::U32),
                }),
                len: NonZeroU32::new(3).assured("three is positive"),
            }
        );
        assert!(model.fields[1].optional);
        assert!(model.fields[1].sensitive);
        assert_canonical_round_trip(Model::Schema(model), false);
    }

    #[test]
    fn ordered_wire_edits_keep_field_identity_and_ignore_stale_field_callbacks() {
        let mut draft = WireSchemaDraft {
            name: "ordered_wire".to_string(),
            mode: Some(WireSchemaStrictness::Strict),
            fields: vec![WireFieldDraft::default(); 3],
            ..WireSchemaDraft::default()
        };
        for (index, name) in ["first", "second", "third"].into_iter().enumerate() {
            draft.set_field_name(index, name.to_string());
            draft.set_field_type(index, WireFieldType::Json(JsonType::String));
        }
        draft.move_field_down(0);
        draft.move_field_up(2);
        draft.remove_field(1);
        draft.set_field_optional(0, true);

        let snapshot = draft.clone();
        draft.move_field_up(0);
        draft.move_field_down(usize::MAX);
        draft.remove_field(usize::MAX);
        draft.set_field_name(usize::MAX, "stale".to_string());
        draft.set_field_type(usize::MAX, WireFieldType::Json(JsonType::String));
        draft.set_field_optional(usize::MAX, false);
        assert_eq!(draft, snapshot);

        let model = draft
            .build(WireFormat::Json)
            .assured("the edited wire fields have types");
        let Model::WireJsonSchema(schema) = &model else {
            panic!("the selected JSON format keeps its model identity");
        };
        assert_eq!(
            schema
                .fields
                .iter()
                .map(|field| field.name.as_ref())
                .collect::<Vec<_>>(),
            ["second", "first"]
        );
        assert!(schema.fields[0].optional);
        assert_canonical_round_trip(model, false);
    }
}
