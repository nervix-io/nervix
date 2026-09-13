//! What a codec can carry, and whether a schema fits through it.
//!
//! Layer: decisions.
//!
//! - **Owns.** The wire schemas a domain declares, the decode and encode capabilities of each
//!   codec, and the field-by-field compatibility between a wire form and an internal schema.
//! - **Depends on.** The codec and wire-schema Models and the schema rules.
//! - **Must not know.** How a codec is instantiated at runtime.

use ahash::{HashSet, HashSetExt};
use error_stack::Report;
use nervix_models::{
    AvroType, CodecEncoding, CodecEncodingRule, CodecName, CodecWireFormat, CreateAvroWireSchema,
    CreateCborWireSchema, CreateCodec, CreateJsonWireSchema, CreateLookup, CreateRelay,
    CreateSchema, CreateWireSchema, DomainName, FieldName, JsonType, LookupName, Model, ModelIndex,
    ModelKind, ModelName, ParseAsType, RelayName, ResolvedCodecWireFormat, WireSchemaLookup,
    WireSchemaName,
};

use crate::registry::{error::RegistryError, validation::schema::expect_schema_model};

/// The wire schemas of a domain's proposed configuration, as a codec's format looks them up.
///
/// The reference the format names is resolved against the store the whole configuration is
/// validated from, and the format decides which kind is read, so a codec never sees a wire schema
/// of another kind.
pub(in crate::registry) struct DomainModelWireSchemas<'a> {
    pub(in crate::registry) domain: &'a DomainName,
    pub(in crate::registry) identifier: &'a ModelName,
    pub(in crate::registry) models: &'a ModelIndex,
}

impl<'a> DomainModelWireSchemas<'a> {
    fn require<T>(
        &self,
        kind: ModelKind,
        referenced: &WireSchemaName,
        extract: impl Fn(&'a Model) -> Option<&'a CreateWireSchema<T>>,
    ) -> Result<&'a CreateWireSchema<T>, Report<RegistryError>> {
        self.models
            .narrowed(kind, referenced.clone(), extract)
            .ok_or_else(|| {
                Report::new(RegistryError::MissingReference {
                    domain: self.domain.as_str().to_string(),
                    identifier: self.identifier.as_str().to_string(),
                    expected_kind: kind.as_str(),
                    reference: referenced.as_str().to_string(),
                })
            })
    }
}

impl WireSchemaLookup for DomainModelWireSchemas<'_> {
    type Error = Report<RegistryError>;

    fn json_wire_schema(
        &self,
        name: &WireSchemaName,
    ) -> Result<&CreateJsonWireSchema, Self::Error> {
        self.require(ModelKind::WireJsonSchema, name, |model| match model {
            Model::WireJsonSchema(schema) => Some(schema),
            _ => None,
        })
    }

    fn cbor_wire_schema(
        &self,
        name: &WireSchemaName,
    ) -> Result<&CreateCborWireSchema, Self::Error> {
        self.require(ModelKind::WireCborSchema, name, |model| match model {
            Model::WireCborSchema(schema) => Some(schema),
            _ => None,
        })
    }

    fn avro_wire_schema(
        &self,
        name: &WireSchemaName,
    ) -> Result<&CreateAvroWireSchema, Self::Error> {
        self.require(ModelKind::WireAvroSchema, name, |model| match model {
            Model::WireAvroSchema(schema) => Some(schema),
            _ => None,
        })
    }
}

pub(in crate::registry) fn expect_codec_model<'a>(
    domain: &DomainName,
    identifier: &ModelName,
    models: &'a ModelIndex,
    referenced: impl Into<ModelName>,
) -> Result<&'a CreateCodec, Report<RegistryError>> {
    let referenced = referenced.into();
    models
        .configured::<CreateCodec>(referenced.clone())
        .ok_or_else(|| {
            Report::new(RegistryError::MissingReference {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                expected_kind: ModelKind::Codec.as_str(),
                reference: referenced.as_str().to_string(),
            })
        })
}

pub(in crate::registry) fn ensure_codec_supports_decoding(
    domain: &DomainName,
    identifier: &ModelName,
    codec: &CreateCodec,
) -> Result<(), Report<RegistryError>> {
    if codec.wire_format.supports_decoding() {
        return Ok(());
    }

    Err(Report::new(RegistryError::InvalidModel {
        domain: domain.as_str().to_string(),
        identifier: identifier.as_str().to_string(),
        reason: format!(
            "codec '{}' cannot be used for decoding because it does not declare an ON INGESTION \
             transformation",
            codec.name.as_str()
        ),
    }))
}

pub(in crate::registry) fn ensure_codec_supports_encoding(
    domain: &DomainName,
    identifier: &ModelName,
    codec: &CreateCodec,
    schema: &CreateSchema,
) -> Result<(), Report<RegistryError>> {
    if !codec.wire_format.supports_encoding() {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!(
                "codec '{}' cannot be used for encoding because it does not declare an ON \
                 EMITTING transformation",
                codec.name.as_str()
            ),
        }));
    }

    if let CodecWireFormat::Syslog = codec.wire_format {
        let missing = ["facility", "severity", "message"]
            .into_iter()
            .filter(|required| {
                !schema
                    .fields
                    .iter()
                    .any(|field| field.name.as_str() == *required)
            })
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            return Err(Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "SYSLOG codec '{}' cannot be used for encoding because schema '{}' is missing \
                     required field{} {}",
                    codec.name.as_str(),
                    schema.name.as_str(),
                    if missing.len() == 1 { "" } else { "s" },
                    missing.join(", "),
                ),
            }));
        }
    }

    Ok(())
}

pub(in crate::registry) fn schema_for_codec_model<'a>(
    domain: &DomainName,
    identifier: &ModelName,
    models: &'a ModelIndex,
    codec_id: &CodecName,
) -> Result<&'a CreateSchema, Report<RegistryError>> {
    let codec = expect_codec_model(domain, identifier, models, codec_id)?;
    expect_schema_model(domain, identifier, models, &codec.schema)
}

pub(in crate::registry) fn schema_for_ack_model<'a>(
    domain: &DomainName,
    identifier: &ModelName,
    models: &'a ModelIndex,
    relay_id: &RelayName,
) -> Result<&'a CreateSchema, Report<RegistryError>> {
    let Some(relay) = models.configured::<CreateRelay>(relay_id.clone()) else {
        return Err(Report::new(RegistryError::MissingReference {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            expected_kind: ModelKind::Relay.as_str(),
            reference: relay_id.as_str().to_string(),
        }));
    };

    expect_schema_model(domain, identifier, models, &relay.schema)
}

pub(in crate::registry) fn schema_for_lookup_model<'a>(
    domain: &DomainName,
    identifier: &ModelName,
    models: &'a ModelIndex,
    lookup_id: &LookupName,
) -> Result<&'a CreateSchema, Report<RegistryError>> {
    let Some(lookup) = models.configured::<CreateLookup>(lookup_id.clone()) else {
        return Err(Report::new(RegistryError::MissingReference {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            expected_kind: ModelKind::Lookup.as_str(),
            reference: lookup_id.as_str().to_string(),
        }));
    };

    schema_for_codec_model(domain, identifier, models, &lookup.decode_using_codec)
}

pub(in crate::registry) fn ensure_codec_schema_compatibility(
    domain: &DomainName,
    identifier: &ModelName,
    wire_format: ResolvedCodecWireFormat<'_>,
    schema: &CreateSchema,
    encoding_rules: &[CodecEncodingRule],
) -> Result<(), Report<RegistryError>> {
    let rfc3339_fields = if let ResolvedCodecWireFormat::Syslog = wire_format {
        if !encoding_rules.is_empty() {
            return Err(Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: "SYSLOG codecs do not support ENCODE field rules".to_string(),
            }));
        }
        HashSet::new()
    } else {
        ensure_supported_codec_encoding_rules(domain, identifier, schema, encoding_rules)?
    };
    match wire_format {
        ResolvedCodecWireFormat::Syslog => ensure_syslog_field_contract(domain, identifier, schema),
        ResolvedCodecWireFormat::Json(json) => ensure_wire_field_set_matches(
            domain,
            identifier,
            &json
                .fields
                .iter()
                .map(|field| WireFieldCompatibility {
                    name: field.name.as_str(),
                    optional: field.optional,
                    wire_type: field.ty.as_ref().to_string(),
                    compatibility: WireTypeCompatibility::Json(field.ty),
                })
                .collect::<Vec<_>>(),
            schema,
            "json",
            &rfc3339_fields,
        ),
        ResolvedCodecWireFormat::Cbor(cbor) => ensure_wire_field_set_matches(
            domain,
            identifier,
            &cbor
                .fields
                .iter()
                .map(|field| WireFieldCompatibility {
                    name: field.name.as_str(),
                    optional: field.optional,
                    wire_type: field.ty.as_ref().to_string(),
                    compatibility: WireTypeCompatibility::Json(field.ty),
                })
                .collect::<Vec<_>>(),
            schema,
            "cbor",
            &rfc3339_fields,
        ),
        ResolvedCodecWireFormat::Avro(avro) => ensure_wire_field_set_matches(
            domain,
            identifier,
            &avro
                .fields
                .iter()
                .map(|field| WireFieldCompatibility {
                    name: field.name.as_str(),
                    optional: field.optional,
                    wire_type: field.ty.as_ref().to_string(),
                    compatibility: WireTypeCompatibility::Avro(field.ty),
                })
                .collect::<Vec<_>>(),
            schema,
            "avro",
            &rfc3339_fields,
        ),
        ResolvedCodecWireFormat::JaqNative {
            transformations, ..
        } => {
            if transformations.has_any() {
                Ok(())
            } else {
                Err(Report::new(RegistryError::InvalidModel {
                    domain: domain.as_str().to_string(),
                    identifier: identifier.as_str().to_string(),
                    reason: "JAQ-native codec must declare a JAQ transformation".to_string(),
                }))
            }
        }
        ResolvedCodecWireFormat::Protobuf(config) => {
            if config.transformations.has_any() {
                Ok(())
            } else {
                Err(Report::new(RegistryError::InvalidModel {
                    domain: domain.as_str().to_string(),
                    identifier: identifier.as_str().to_string(),
                    reason: "protobuf codec must declare a JAQ transformation".to_string(),
                }))
            }
        }
    }
}

fn ensure_syslog_field_contract(
    domain: &DomainName,
    identifier: &ModelName,
    schema: &CreateSchema,
) -> Result<(), Report<RegistryError>> {
    for field in &schema.fields {
        let expected = match field.name.as_str() {
            "facility" | "severity" => Some((ParseAsType::U8, false)),
            "timestamp" => Some((ParseAsType::Datetime, true)),
            "hostname" | "app_name" | "proc_id" | "msg_id" | "structured_data" => {
                Some((ParseAsType::String, true))
            }
            "message" => Some((ParseAsType::String, false)),
            _ => None,
        };
        let Some((expected_type, expected_optional)) = expected else {
            return Err(Report::new(RegistryError::IncompatibleSchema {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "SYSLOG schema field '{}' is outside the fixed field contract",
                    field.name.as_str()
                ),
            }));
        };
        if field.ty != expected_type || field.optional != expected_optional {
            return Err(Report::new(RegistryError::IncompatibleSchema {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "SYSLOG field '{}' must be {}{}, found {}{}",
                    field.name.as_str(),
                    expected_type,
                    if expected_optional { " OPTIONAL" } else { "" },
                    field.ty,
                    if field.optional { " OPTIONAL" } else { "" },
                ),
            }));
        }
    }
    Ok(())
}

fn ensure_supported_codec_encoding_rules(
    domain: &DomainName,
    identifier: &ModelName,
    schema: &CreateSchema,
    encoding_rules: &[CodecEncodingRule],
) -> Result<HashSet<FieldName>, Report<RegistryError>> {
    let mut rfc3339_fields = HashSet::new();
    for rule in encoding_rules {
        if rule.encoding != CodecEncoding::Rfc3339 {
            return Err(Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!("unsupported codec encoding rule {rule:?}"),
            }));
        }

        let Some(schema_field) = schema
            .fields
            .iter()
            .find(|schema_field| schema_field.name == rule.field)
        else {
            return Err(Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "codec encoding rule references unknown schema field '{}'",
                    rule.field.as_str()
                ),
            }));
        };

        if schema_field.ty != ParseAsType::Datetime {
            return Err(Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "codec encoding rule field '{}' must be DATETIME, found {:?}",
                    rule.field.as_str(),
                    schema_field.ty
                ),
            }));
        }

        if !rfc3339_fields.insert(rule.field.clone()) {
            return Err(Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "duplicate codec encoding rule for field '{}'",
                    rule.field.as_str()
                ),
            }));
        }
    }
    Ok(rfc3339_fields)
}

struct WireFieldCompatibility<'a> {
    name: &'a str,
    optional: bool,
    wire_type: String,
    compatibility: WireTypeCompatibility,
}

#[derive(Clone, Copy)]
enum WireTypeCompatibility {
    Json(JsonType),
    Avro(AvroType),
}

fn ensure_wire_field_set_matches(
    domain: &DomainName,
    identifier: &ModelName,
    wire_fields: &[WireFieldCompatibility<'_>],
    schema: &CreateSchema,
    wire_kind: &str,
    rfc3339_fields: &HashSet<FieldName>,
) -> Result<(), Report<RegistryError>> {
    for schema_field in &schema.fields {
        let Some(wire_field) = wire_fields
            .iter()
            .find(|wire_field| wire_field.name == schema_field.name.as_str())
        else {
            return Err(Report::new(RegistryError::IncompatibleSchema {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "{wire_kind} wire schema is missing field '{}'",
                    schema_field.name.as_str()
                ),
            }));
        };

        if !wire_field.compatibility.supports(
            &schema_field.ty,
            rfc3339_fields.contains(&schema_field.name),
        ) {
            return Err(Report::new(RegistryError::IncompatibleSchema {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "{wire_kind} field '{}' type mismatch: wire {}, internal {:?}",
                    schema_field.name.as_str(),
                    wire_field.wire_type,
                    schema_field.ty
                ),
            }));
        }
        if wire_field.optional != schema_field.optional {
            return Err(Report::new(RegistryError::IncompatibleSchema {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "{wire_kind} field '{}' optionality mismatch: wire {}, internal {}",
                    schema_field.name.as_str(),
                    wire_field.optional,
                    schema_field.optional
                ),
            }));
        }
    }

    if wire_fields.len() != schema.fields.len() {
        return Err(Report::new(RegistryError::IncompatibleSchema {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!(
                "{wire_kind} wire schema field set must exactly match internal schema '{}'",
                schema.name.as_str()
            ),
        }));
    }

    Ok(())
}

impl WireTypeCompatibility {
    fn supports(self, ty: &ParseAsType, encodes_datetime_as_rfc3339: bool) -> bool {
        match self {
            Self::Json(wire) => json_type_matches_parse_as(wire, ty, encodes_datetime_as_rfc3339),
            Self::Avro(wire) => avro_type_matches_parse_as(wire, ty, encodes_datetime_as_rfc3339),
        }
    }
}

fn json_type_matches_parse_as(
    wire: JsonType,
    ty: &ParseAsType,
    encodes_datetime_as_rfc3339: bool,
) -> bool {
    match wire {
        JsonType::String => {
            *ty == ParseAsType::String
                || encodes_datetime_as_rfc3339 && *ty == ParseAsType::Datetime
        }
        JsonType::Number => *ty == ParseAsType::F32 || *ty == ParseAsType::F64,
        JsonType::Integer => parse_as_is_integer(ty),
        JsonType::Boolean => *ty == ParseAsType::Bool,
        JsonType::Array => parse_as_is_list(ty),
        JsonType::Object
        | JsonType::Null
        | JsonType::U8
        | JsonType::I8
        | JsonType::U16
        | JsonType::I16
        | JsonType::U32
        | JsonType::I32
        | JsonType::U64
        | JsonType::I64
        | JsonType::Datetime
        | JsonType::F32
        | JsonType::F64 => false,
    }
}

fn avro_type_matches_parse_as(
    wire: AvroType,
    ty: &ParseAsType,
    encodes_datetime_as_rfc3339: bool,
) -> bool {
    match wire {
        AvroType::Boolean => *ty == ParseAsType::Bool,
        AvroType::Int => *ty == ParseAsType::I32,
        AvroType::Long => *ty == ParseAsType::I64,
        AvroType::Float => *ty == ParseAsType::F32,
        AvroType::Double => *ty == ParseAsType::F64,
        AvroType::String => {
            *ty == ParseAsType::String
                || encodes_datetime_as_rfc3339 && *ty == ParseAsType::Datetime
        }
        AvroType::Array => parse_as_is_list(ty),
        AvroType::Null
        | AvroType::Bytes
        | AvroType::Record
        | AvroType::Enum
        | AvroType::Map
        | AvroType::Fixed => false,
    }
}

fn parse_as_is_list(ty: &ParseAsType) -> bool {
    if let ParseAsType::Array { .. } = ty {
        return true;
    }
    if let ParseAsType::Vec { .. } = ty {
        return true;
    }
    false
}

fn parse_as_is_integer(ty: &ParseAsType) -> bool {
    matches!(
        ty,
        ParseAsType::U8
            | ParseAsType::I8
            | ParseAsType::U16
            | ParseAsType::I16
            | ParseAsType::U32
            | ParseAsType::I32
            | ParseAsType::U64
            | ParseAsType::I64
    )
}

#[cfg(test)]
mod tests {
    use std::fs;

    use nervix_models::{EmitSink, SchemaField, SchemaName, WireSchemaField};
    use rstest::rstest;

    use super::*;
    use crate::registry::{
        storage::Registry,
        test_fixtures::{
            avro_codec, avro_wire_schema_with_type, client_model, codec, emitter, ingestor,
            jaq_native_codec, json_wire_schema_with_type, named, protobuf_codec, relay,
            rfc3339_json_codec_for_field, schema, syslog_client, syslog_codec, temp_db_path,
            wire_schema,
        },
    };
    #[derive(Debug)]
    enum CodecTypeCase {
        Json {
            internal: ParseAsType,
            wire: JsonType,
            rfc3339_field: Option<&'static str>,
        },
        Avro {
            internal: ParseAsType,
            wire: nervix_models::AvroType,
        },
    }

    impl CodecTypeCase {
        fn models(self) -> Vec<Model> {
            let internal = match &self {
                Self::Json { internal, .. } | Self::Avro { internal, .. } => internal.clone(),
            };
            let schema = Model::Schema(CreateSchema {
                name: named("event_schema"),
                fields: vec![SchemaField {
                    name: named("value"),
                    ty: internal,
                    optional: false,
                    sensitive: false,
                }],
            });

            match self {
                Self::Json {
                    wire,
                    rfc3339_field,
                    ..
                } => {
                    let codec = if let Some(field) = rfc3339_field {
                        rfc3339_json_codec_for_field(
                            "event_codec",
                            "event_wire",
                            "event_schema",
                            field,
                        )
                    } else {
                        codec("event_codec", "event_schema")
                    };
                    vec![
                        schema,
                        json_wire_schema_with_type("event_wire", wire),
                        codec,
                    ]
                }
                Self::Avro { wire, .. } => vec![
                    schema,
                    avro_wire_schema_with_type("event_wire", wire),
                    avro_codec("event_codec", "event_wire", "event_schema"),
                ],
            }
        }
    }

    #[derive(Debug)]
    enum CodecTypeExpectation {
        Accepted,
        IncompatibleSchema,
        InvalidModel,
    }

    #[test]
    fn ingestor_rejects_codec_without_decode_capability() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");

        let error = registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    jaq_native_codec("event_codec", "event_schema", None, Some("{payload: .}")),
                    client_model("kafka_main"),
                    relay("notifications", "event_schema"),
                    ingestor("ing", "notifications", "event_codec", "kafka_main"),
                ],
            )
            .expect_err("ingestor must reject encode-only codec");

        assert!(
            format!("{error:#}").contains(
                "codec 'event_codec' cannot be used for decoding because it does not declare an \
                 ON INGESTION transformation"
            ),
            "unexpected error: {error:#}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn emitter_rejects_codec_without_encode_capability() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");

        let error = registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    jaq_native_codec("event_codec", "event_schema", Some("."), None),
                    client_model("broker_out"),
                    relay("notifications", "event_schema"),
                    emitter("emit", "notifications", "event_codec", "broker_out"),
                ],
            )
            .expect_err("emitter must reject decode-only codec");

        assert!(
            format!("{error:#}").contains(
                "codec 'event_codec' cannot be used for encoding because it does not declare an \
                 ON EMITTING transformation"
            ),
            "unexpected error: {error:#}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_incompatible_codec_schema() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");

        let err = registry
            .apply_batch(
                &domain,
                vec![
                    Model::Schema(CreateSchema {
                        name: SchemaName::parse("event_schema").expect("valid identifier"),
                        fields: vec![SchemaField {
                            name: FieldName::parse("value").expect("valid identifier"),
                            ty: nervix_models::ParseAsType::U32,
                            optional: false,
                            sensitive: false,
                        }],
                    }),
                    wire_schema("event_wire"),
                    codec("event_codec", "event_schema"),
                ],
            )
            .expect_err("incompatible codec schema should fail");

        assert!(matches!(
            err.current_context(),
            RegistryError::IncompatibleSchema { .. }
        ));

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn syslog_codec_accepts_an_exact_subset_of_its_field_contract() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");

        registry
            .apply_batch(
                &domain,
                vec![
                    Model::Schema(CreateSchema {
                        name: named("syslog_event"),
                        fields: vec![
                            SchemaField {
                                name: named("facility"),
                                ty: ParseAsType::U8,
                                optional: false,
                                sensitive: false,
                            },
                            SchemaField {
                                name: named("timestamp"),
                                ty: ParseAsType::Datetime,
                                optional: true,
                                sensitive: true,
                            },
                            SchemaField {
                                name: named("message"),
                                ty: ParseAsType::String,
                                optional: false,
                                sensitive: false,
                            },
                        ],
                    }),
                    syslog_codec("syslog_codec", "syslog_event"),
                ],
            )
            .expect("an exact SYSLOG field subset should be accepted");

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn syslog_codec_rejects_fields_outside_or_mismatching_its_contract() {
        for (field, expected_reason) in [
            (
                SchemaField {
                    name: named("facility"),
                    ty: ParseAsType::U16,
                    optional: false,
                    sensitive: false,
                },
                "SYSLOG field 'facility' must be U8",
            ),
            (
                SchemaField {
                    name: named("hostname"),
                    ty: ParseAsType::String,
                    optional: false,
                    sensitive: false,
                },
                "SYSLOG field 'hostname' must be STRING OPTIONAL",
            ),
            (
                SchemaField {
                    name: named("payload"),
                    ty: ParseAsType::String,
                    optional: false,
                    sensitive: false,
                },
                "SYSLOG schema field 'payload' is outside the fixed field contract",
            ),
        ] {
            let path = temp_db_path();
            let registry = Registry::open(&path).expect("registry should open");
            let domain = DomainName::parse("default").expect("valid domain");
            let error = registry
                .apply_batch(
                    &domain,
                    vec![
                        Model::Schema(CreateSchema {
                            name: named("syslog_event"),
                            fields: vec![field],
                        }),
                        syslog_codec("syslog_codec", "syslog_event"),
                    ],
                )
                .expect_err("invalid SYSLOG schema field must be rejected");
            assert!(
                format!("{error:#}").contains(expected_reason),
                "unexpected error: {error:#}"
            );
            let _ = fs::remove_dir_all(path);
        }
    }

    #[test]
    fn syslog_emitter_requires_priority_and_message_fields() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");
        let Model::Emitter(mut emitter) = emitter("emit", "events", "syslog_codec", "syslog_out")
        else {
            unreachable!("emitter helper must build an emitter")
        };
        emitter.sink = Box::new(EmitSink::Syslog {
            client: named("syslog_out"),
        });

        let error = registry
            .apply_batch(
                &domain,
                vec![
                    Model::Schema(CreateSchema {
                        name: named("syslog_event"),
                        fields: vec![SchemaField {
                            name: named("message"),
                            ty: ParseAsType::String,
                            optional: false,
                            sensitive: false,
                        }],
                    }),
                    syslog_codec("syslog_codec", "syslog_event"),
                    syslog_client("syslog_out"),
                    relay("events", "syslog_event"),
                    Model::Emitter(emitter),
                ],
            )
            .expect_err("SYSLOG emitter codec must declare facility and severity");
        let rendered = format!("{error:#}");
        assert!(
            rendered.contains("missing required fields facility, severity"),
            "{rendered}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[rstest]
    #[case::requires_explicit_rfc3339_for_json_string_datetime(
        CodecTypeCase::Json {
            internal: ParseAsType::Datetime,
            wire: JsonType::String,
            rfc3339_field: None,
        },
        CodecTypeExpectation::IncompatibleSchema
    )]
    #[case::accepts_explicit_rfc3339_for_json_string_datetime(
        CodecTypeCase::Json {
            internal: ParseAsType::Datetime,
            wire: JsonType::String,
            rfc3339_field: Some("value"),
        },
        CodecTypeExpectation::Accepted
    )]
    #[case::rejects_rfc3339_for_unknown_field(
        CodecTypeCase::Json {
            internal: ParseAsType::Datetime,
            wire: JsonType::String,
            rfc3339_field: Some("missing"),
        },
        CodecTypeExpectation::InvalidModel
    )]
    #[case::rejects_rfc3339_for_non_datetime_field(
        CodecTypeCase::Json {
            internal: ParseAsType::String,
            wire: JsonType::String,
            rfc3339_field: Some("value"),
        },
        CodecTypeExpectation::InvalidModel
    )]
    #[case::rejects_rfc3339_without_json_string_wire_datetime(
        CodecTypeCase::Json {
            internal: ParseAsType::Datetime,
            wire: JsonType::Number,
            rfc3339_field: Some("value"),
        },
        CodecTypeExpectation::IncompatibleSchema
    )]
    #[case::accepts_json_integer_for_internal_u32(
        CodecTypeCase::Json {
            internal: ParseAsType::U32,
            wire: JsonType::Integer,
            rfc3339_field: None,
        },
        CodecTypeExpectation::Accepted
    )]
    #[case::accepts_json_number_for_internal_f32(
        CodecTypeCase::Json {
            internal: ParseAsType::F32,
            wire: JsonType::Number,
            rfc3339_field: None,
        },
        CodecTypeExpectation::Accepted
    )]
    #[case::rejects_avro_long_internal_i32_coercion(
        CodecTypeCase::Avro {
            internal: ParseAsType::I32,
            wire: nervix_models::AvroType::Long,
        },
        CodecTypeExpectation::IncompatibleSchema
    )]
    fn validates_codec_schema_type_matrix(
        #[case] codec_type: CodecTypeCase,
        #[case] expected: CodecTypeExpectation,
    ) {
        let case = format!("{codec_type:?}");
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");
        let result = registry.apply_batch(&domain, codec_type.models());

        match expected {
            CodecTypeExpectation::Accepted => {
                result.unwrap_or_else(|error| panic!("{case} should be accepted: {error:#}"));
            }
            CodecTypeExpectation::IncompatibleSchema => {
                let error = result.expect_err("incompatible codec types must be rejected");
                assert!(
                    matches!(
                        error.current_context(),
                        RegistryError::IncompatibleSchema { .. }
                    ),
                    "unexpected error for {case}: {error:#}"
                );
            }
            CodecTypeExpectation::InvalidModel => {
                let error = result.expect_err("invalid codec configuration must be rejected");
                assert!(
                    matches!(error.current_context(), RegistryError::InvalidModel { .. }),
                    "unexpected error for {case}: {error:#}"
                );
            }
        }

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_wire_and_internal_optionality_mismatch() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");

        let err = registry
            .apply_batch(
                &domain,
                vec![
                    Model::Schema(CreateSchema {
                        name: SchemaName::parse("event_schema").expect("valid identifier"),
                        fields: vec![SchemaField {
                            name: FieldName::parse("value").expect("valid identifier"),
                            ty: nervix_models::ParseAsType::String,
                            optional: false,
                            sensitive: false,
                        }],
                    }),
                    Model::WireJsonSchema(CreateWireSchema {
                        name: WireSchemaName::parse("event_wire").expect("valid identifier"),
                        strictness: Default::default(),
                        fields: vec![WireSchemaField {
                            name: FieldName::parse("value").expect("valid identifier"),
                            ty: JsonType::String,
                            optional: true,
                        }],
                    }),
                    codec("event_codec", "event_schema"),
                ],
            )
            .expect_err("wire/internal optionality mismatch should fail");

        assert!(
            format!("{err:#}").contains("optionality mismatch"),
            "unexpected error: {err:#}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn ingestor_rejects_protobuf_codec_without_decode_capability() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");

        let error = registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    protobuf_codec("event_codec", "event_schema", None, Some("{payload: .}")),
                    client_model("kafka_main"),
                    relay("notifications", "event_schema"),
                    ingestor("ing", "notifications", "event_codec", "kafka_main"),
                ],
            )
            .expect_err("ingestor must reject encode-only protobuf codec");

        assert!(
            format!("{error:#}").contains(
                "codec 'event_codec' cannot be used for decoding because it does not declare an \
                 ON INGESTION transformation"
            ),
            "unexpected error: {error:#}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn emitter_rejects_protobuf_codec_without_encode_capability() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");

        let error = registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    protobuf_codec("event_codec", "event_schema", Some("."), None),
                    client_model("broker_out"),
                    relay("notifications", "event_schema"),
                    emitter("emit", "notifications", "event_codec", "broker_out"),
                ],
            )
            .expect_err("emitter must reject decode-only protobuf codec");

        assert!(
            format!("{error:#}").contains(
                "codec 'event_codec' cannot be used for encoding because it does not declare an \
                 ON EMITTING transformation"
            ),
            "unexpected error: {error:#}"
        );

        let _ = fs::remove_dir_all(path);
    }
}
