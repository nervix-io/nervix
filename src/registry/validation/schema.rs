//! What a schema means to the VM, and when two of them are compatible.
//!
//! Layer: decisions.
//!
//! - **Owns.** The Arrow shape and sensitivity of a declared schema, the bindings a program
//!   compiles against it, and the exact-type and sensitivity rules two schemas must satisfy.
//! - **Depends on.** The schema Models and the VM's type system.
//! - **Must not know.** Which model asked for the comparison.

use std::sync::Arc as StdArc;

use arrow_schema::{
    DataType as ArrowDataType, Field as ArrowField, FieldRef as ArrowFieldRef,
    Schema as ArrowSchema, TimeUnit as ArrowTimeUnit,
};
use error_stack::Report;
use meticulous::ResultExt;
use nervix_models::{
    CreateSchema, CreateWireSchema, DomainName, ModelIndex, ModelKind, ModelName, ParseAsType,
    SchemaField, SchemaName,
};
use nervix_vm::{CompileBinding, SchemaSensitivity};

use crate::registry::error::RegistryError;

pub(in crate::registry) fn ensure_schema_has_fields<T>(
    domain: &DomainName,
    identifier: &ModelName,
    fields: &[T],
    schema_kind: &str,
) -> Result<(), Report<RegistryError>> {
    if fields.is_empty() {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("{schema_kind} must declare at least one field"),
        }));
    }
    Ok(())
}

pub(in crate::registry) fn ensure_wire_schema_has_fields<T>(
    domain: &DomainName,
    identifier: &ModelName,
    schema: &CreateWireSchema<T>,
) -> Result<(), Report<RegistryError>> {
    ensure_schema_has_fields(domain, identifier, &schema.fields, "wire schema")
}

pub(in crate::registry) fn expect_schema_model<'a>(
    domain: &DomainName,
    identifier: &ModelName,
    models: &'a ModelIndex,
    referenced: &SchemaName,
) -> Result<&'a CreateSchema, Report<RegistryError>> {
    models
        .configured::<CreateSchema>(referenced.clone())
        .ok_or_else(|| {
            Report::new(RegistryError::MissingReference {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                expected_kind: ModelKind::Schema.as_str(),
                reference: referenced.as_str().to_string(),
            })
        })
}

pub(in crate::registry) fn arrow_schema_for_internal_schema(
    schema: &CreateSchema,
) -> StdArc<ArrowSchema> {
    StdArc::new(ArrowSchema::new(
        schema
            .fields
            .iter()
            .map(arrow_field_for_schema_field)
            .collect::<Vec<_>>(),
    ))
}

pub(in crate::registry) fn arrow_field_for_schema_field(field: &SchemaField) -> ArrowField {
    ArrowField::new(
        field.name.as_str(),
        arrow_data_type_for_parse_as(&field.ty),
        field.optional,
    )
}

pub(in crate::registry) fn schema_sensitivity_for_internal_schema(
    schema: &CreateSchema,
) -> SchemaSensitivity {
    SchemaSensitivity::from_sensitive_fields(
        schema
            .fields
            .iter()
            .filter(|field| field.sensitive)
            .map(|field| field.name.as_str().to_string()),
    )
}

pub(in crate::registry) fn compile_binding_with_internal_schema(
    binding: CompileBinding,
    schema: &CreateSchema,
) -> CompileBinding {
    binding.with_sensitivity(schema_sensitivity_for_internal_schema(schema))
}

pub(in crate::registry) fn writable_binding_for_internal_schema(
    namespace: impl Into<String>,
    schema: &CreateSchema,
) -> CompileBinding {
    compile_binding_with_internal_schema(
        CompileBinding::writable(namespace, arrow_schema_for_internal_schema(schema)),
        schema,
    )
}

pub(in crate::registry) fn readonly_binding_for_internal_schema(
    namespace: impl Into<String>,
    schema: &CreateSchema,
) -> CompileBinding {
    compile_binding_with_internal_schema(
        CompileBinding::readonly(namespace, arrow_schema_for_internal_schema(schema)),
        schema,
    )
}

pub(in crate::registry) fn arrow_data_type_for_parse_as(ty: &ParseAsType) -> ArrowDataType {
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
            ArrowFieldRef::new(ArrowField::new(
                "item",
                arrow_data_type_for_parse_as(element),
                false,
            )),
            i32::try_from(len.get()).verified(
                "the schema parser rejects an array length that does not fit an Arrow fixed-size \
                 list",
            ),
        ),
        ParseAsType::Vec { element } => ArrowDataType::List(ArrowFieldRef::new(ArrowField::new(
            "item",
            arrow_data_type_for_parse_as(element),
            false,
        ))),
    }
}

pub(in crate::registry) fn ensure_internal_schema_compatibility(
    domain: &DomainName,
    identifier: &ModelName,
    producer: &CreateSchema,
    consumer: &CreateSchema,
    relation: &str,
) -> Result<(), Report<RegistryError>> {
    ensure_internal_schema_compatibility_with_policy(
        domain,
        identifier,
        producer,
        consumer,
        relation,
        SensitivityCompatibility::Enforce,
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::registry) enum SensitivityCompatibility {
    Enforce,
    AllowSensitiveProducer,
}

pub(in crate::registry) fn ensure_internal_schema_compatibility_with_policy(
    domain: &DomainName,
    identifier: &ModelName,
    producer: &CreateSchema,
    consumer: &CreateSchema,
    relation: &str,
    sensitivity: SensitivityCompatibility,
) -> Result<(), Report<RegistryError>> {
    for consumer_field in &consumer.fields {
        let Some(producer_field) = producer
            .fields
            .iter()
            .find(|field| field.name == consumer_field.name)
        else {
            return Err(Report::new(RegistryError::IncompatibleSchema {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "{relation} requires producer schema '{}' to provide field '{}'",
                    producer.name.as_str(),
                    consumer_field.name.as_str()
                ),
            }));
        };

        if producer_field.ty != consumer_field.ty {
            return Err(Report::new(RegistryError::IncompatibleSchema {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "{relation} field '{}' type mismatch: producer {:?}, consumer {:?}",
                    consumer_field.name.as_str(),
                    producer_field.ty,
                    consumer_field.ty
                ),
            }));
        }
        if producer_field.optional != consumer_field.optional {
            return Err(Report::new(RegistryError::IncompatibleSchema {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "{relation} field '{}' optionality mismatch: producer {}, consumer {}",
                    consumer_field.name.as_str(),
                    producer_field.optional,
                    consumer_field.optional
                ),
            }));
        }
        if producer_field.sensitive
            && !consumer_field.sensitive
            && sensitivity == SensitivityCompatibility::Enforce
        {
            return Err(Report::new(RegistryError::IncompatibleSchema {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "{relation} field '{}' would store sensitive data in a non-sensitive output \
                     field; use leak_sensitive(...) to explicitly remove sensitivity",
                    consumer_field.name.as_str()
                ),
            }));
        }
    }

    for producer_field in &producer.fields {
        if consumer
            .fields
            .iter()
            .any(|field| field.name == producer_field.name)
        {
            continue;
        }

        return Err(Report::new(RegistryError::IncompatibleSchema {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!(
                "{relation} produces field '{}' that is not declared in consumer schema '{}'",
                producer_field.name.as_str(),
                consumer.name.as_str()
            ),
        }));
    }

    Ok(())
}

pub(in crate::registry) fn ensure_equal_internal_schema(
    domain: &DomainName,
    identifier: &ModelName,
    left: &CreateSchema,
    right: &CreateSchema,
    relation: &str,
) -> Result<(), Report<RegistryError>> {
    if left.fields == right.fields {
        return Ok(());
    }

    Err(Report::new(RegistryError::IncompatibleSchema {
        domain: domain.as_str().to_string(),
        identifier: identifier.as_str().to_string(),
        reason: format!(
            "{relation} requires equal internal schemas, but '{}' and '{}' differ",
            left.name.as_str(),
            right.name.as_str()
        ),
    }))
}

pub(in crate::registry) fn all_optional_binding_for_internal_schema(
    namespace: impl Into<String>,
    schema: &CreateSchema,
) -> CompileBinding {
    CompileBinding::readonly(
        namespace,
        StdArc::new(ArrowSchema::new(
            schema
                .fields
                .iter()
                .map(|field| {
                    ArrowField::new(
                        field.name.as_str(),
                        arrow_data_type_for_parse_as(&field.ty),
                        true,
                    )
                })
                .collect::<Vec<_>>(),
        )),
    )
    .with_sensitivity(schema_sensitivity_for_internal_schema(schema))
}

pub(in crate::registry) fn structured_message_error_arrow_schema() -> StdArc<ArrowSchema> {
    StdArc::new(ArrowSchema::new(vec![
        ArrowField::new("reference", ArrowDataType::Utf8, false),
        ArrowField::new("code", ArrowDataType::Utf8, false),
        ArrowField::new("message", ArrowDataType::Utf8, false),
        ArrowField::new("operation", ArrowDataType::Utf8, false),
        ArrowField::new("operation_index", ArrowDataType::UInt32, true),
        ArrowField::new(
            "fields",
            ArrowDataType::List(StdArc::new(ArrowField::new(
                "item",
                ArrowDataType::Utf8,
                false,
            ))),
            false,
        ),
        ArrowField::new(
            "occurred_at",
            ArrowDataType::Timestamp(ArrowTimeUnit::Nanosecond, Some("+00:00".into())),
            false,
        ),
    ]))
}

#[cfg(test)]
mod tests {
    use std::fs;

    use nervix_models::{
        AckMode, AlterSchema, AlterSchemaOperation, CreateReingestor, FieldName, JsonType, Model,
        ProcessorInputs, WireSchemaField,
    };
    use nonzero_ext::nonzero;

    use super::*;
    use crate::registry::{
        mutation::RegistryMutation,
        storage::Registry,
        test_fixtures::{
            branch_for_relay, branch_schema, codec, explicitly_unbranched_relay, junction, named,
            relay, schema, temp_db_path, unbranched_transforming_outputs, wire_schema,
        },
    };

    #[test]
    fn apply_batch_rejects_empty_schemas() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let schema_domain = DomainName::parse("empty_schema").expect("valid domain");
        let wire_schema_domain = DomainName::parse("empty_wire_schema").expect("valid domain");

        let result = registry.apply_batch(
            &schema_domain,
            vec![Model::Schema(CreateSchema {
                name: named("root_branch"),
                fields: Vec::new(),
            })],
        );
        assert!(matches!(
            result
                .expect_err("empty schema should be rejected")
                .current_context(),
            RegistryError::InvalidModel { .. }
        ));

        let result = registry.apply_batch(
            &wire_schema_domain,
            vec![Model::WireJsonSchema(CreateWireSchema {
                name: named("empty_wire"),
                strictness: Default::default(),
                fields: Vec::<WireSchemaField<JsonType>>::new(),
            })],
        );
        assert!(matches!(
            result
                .expect_err("empty wire schema should be rejected")
                .current_context(),
            RegistryError::InvalidModel { .. }
        ));

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_sensitive_passthrough_to_non_sensitive_field() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");

        let err = registry
            .apply_batch(
                &domain,
                vec![
                    Model::Schema(CreateSchema {
                        name: named("sensitive_event"),
                        fields: vec![
                            SchemaField {
                                name: named("user_id"),
                                ty: ParseAsType::I64,
                                optional: false,
                                sensitive: false,
                            },
                            SchemaField {
                                name: named("secret"),
                                ty: ParseAsType::String,
                                optional: false,
                                sensitive: true,
                            },
                        ],
                    }),
                    Model::Schema(CreateSchema {
                        name: named("public_event"),
                        fields: vec![
                            SchemaField {
                                name: named("user_id"),
                                ty: ParseAsType::I64,
                                optional: false,
                                sensitive: false,
                            },
                            SchemaField {
                                name: named("secret"),
                                ty: ParseAsType::String,
                                optional: false,
                                sensitive: false,
                            },
                        ],
                    }),
                    explicitly_unbranched_relay("sensitive_events", "sensitive_event"),
                    explicitly_unbranched_relay("public_events", "public_event"),
                    Model::Reingestor(CreateReingestor {
                        name: named("leak_events"),
                        from: ProcessorInputs::single(named("sensitive_events")),
                        output_routes: unbranched_transforming_outputs("public_events"),
                        mode: AckMode::Attached,
                        filter_where: None,
                        materialized_state: Vec::new(),
                    }),
                ],
            )
            .expect_err("sensitive passthrough into public schema should fail");

        let message = format!("{err:#}");
        assert!(
            message.contains("would store sensitive data in a non-sensitive output field"),
            "unexpected error: {message}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_incompatible_array_lengths() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");

        let err = registry
            .apply_batch(
                &domain,
                vec![
                    Model::Schema(CreateSchema {
                        name: SchemaName::parse("short_schema").expect("valid identifier"),
                        fields: vec![SchemaField {
                            name: FieldName::parse("window").expect("valid identifier"),
                            ty: nervix_models::ParseAsType::Array {
                                element: Box::new(nervix_models::ParseAsType::F32),
                                len: nonzero!(2u32),
                            },
                            optional: false,
                            sensitive: false,
                        }],
                    }),
                    Model::Schema(CreateSchema {
                        name: SchemaName::parse("long_schema").expect("valid identifier"),
                        fields: vec![SchemaField {
                            name: FieldName::parse("window").expect("valid identifier"),
                            ty: nervix_models::ParseAsType::Array {
                                element: Box::new(nervix_models::ParseAsType::F32),
                                len: nonzero!(3u32),
                            },
                            optional: false,
                            sensitive: false,
                        }],
                    }),
                    relay("short_stream", "short_schema"),
                    relay("long_stream", "long_schema"),
                    relay("merged", "short_schema"),
                    branch_schema("window_branch", &["window"]),
                    branch_for_relay("short_stream", "window_branch"),
                    junction("merge_windows", &["short_stream", "long_stream"], "merged"),
                ],
            )
            .expect_err("array length mismatch should fail");

        assert!(
            format!("{err:#}").contains("differ"),
            "unexpected error: {err:#}"
        );
        assert!(matches!(
            err.current_context(),
            RegistryError::IncompatibleSchema { .. }
        ));

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn schema_alter_revalidates_dependent_codec() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");
        registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    wire_schema("event_wire"),
                    codec("event_codec", "event_schema"),
                ],
            )
            .expect("create should succeed");

        let error = registry
            .apply_mutation_batch(
                &domain,
                vec![RegistryMutation::AlterSchema(AlterSchema {
                    schema: named("event_schema"),
                    operations: vec![AlterSchemaOperation::SetFieldType {
                        field: named("value"),
                        ty: ParseAsType::F64,
                    }],
                })],
            )
            .expect_err("codec incompatibility should reject ALTER");
        assert!(matches!(
            error.current_context(),
            RegistryError::IncompatibleSchema { .. }
        ));

        let schema = registry
            .get::<CreateSchema>(&domain, named::<ModelName>("event_schema"))
            .expect("read should succeed")
            .expect("schema should exist");
        assert_eq!(schema.fields[0].ty, ParseAsType::String);

        let _ = fs::remove_dir_all(path);
    }
}
