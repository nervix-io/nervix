//! What a codec can carry, and whether a schema fits through it.
//!
//! Layer: decisions.
//!
//! - **Owns.** The wire schemas a domain declares, the decode and encode capabilities of each
//!   codec, and the field-by-field compatibility between a wire form and an internal schema.
//! - **Depends on.** The codec and wire-schema Models and the schema rules.
//! - **Must not know.** How a codec is instantiated at runtime.

#![cfg_attr(
    nervix_lint,
    nervix::layer(decisions, reason = "owns the decisions boundary declared above")
)]

use std::fmt;

use ahash::{HashMap, HashMapExt};
use error_stack::Report;
use nervix_jaq::CompiledJaqProgram;
use nervix_models::{
    AvroType, CborType, CodecEncoding, CodecEncodingRule, CodecJaqTransformations, CodecName,
    CodecWireFormat, CreateAvroWireSchema, CreateCborWireSchema, CreateCodec, CreateJsonWireSchema,
    CreateLookup, CreateRelay, CreateSchema, CreateWireSchema, DomainName, FieldName,
    IngestorInput, JsonType, LookupName, Model, ModelIndex, ModelKind, ModelName, ParseAsType,
    RelayName, ResolvedCodecWireFormat, WireSchemaField, WireSchemaLookup, WireSchemaName,
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
    ) -> Result<nervix_models::WireSchemaRef<'_, nervix_models::JsonType>, Self::Error> {
        self.require(ModelKind::WireJsonSchema, name, |model| match model {
            Model::WireJsonSchema(schema) => Some(schema),
            _ => None,
        })
        .map(CreateWireSchema::definition)
    }

    fn cbor_wire_schema(
        &self,
        name: &WireSchemaName,
    ) -> Result<nervix_models::WireSchemaRef<'_, nervix_models::CborType>, Self::Error> {
        self.require(ModelKind::WireCborSchema, name, |model| match model {
            Model::WireCborSchema(schema) => Some(schema),
            _ => None,
        })
        .map(CreateWireSchema::definition)
    }

    fn avro_wire_schema(
        &self,
        name: &WireSchemaName,
    ) -> Result<nervix_models::WireSchemaRef<'_, nervix_models::AvroType>, Self::Error> {
        self.require(ModelKind::WireAvroSchema, name, |model| match model {
            Model::WireAvroSchema(schema) => Some(schema),
            _ => None,
        })
        .map(CreateWireSchema::definition)
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

/// The input schema of an ingestor: what its codec decodes a transport's payloads into, or the
/// schema a client ingestor's batches carry. Filters, routes and timestamps read this schema.
pub(in crate::registry) fn ingestor_input_schema<'a>(
    domain: &DomainName,
    identifier: &ModelName,
    models: &'a ModelIndex,
    input: &IngestorInput,
) -> Result<&'a CreateSchema, Report<RegistryError>> {
    match input {
        IngestorInput::Transport(input) => {
            schema_for_codec_model(domain, identifier, models, &input.codec)
        }
        IngestorInput::Client(source) => {
            expect_schema_model(domain, identifier, models, &source.schema)
        }
    }
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
    let field_encodings = if let ResolvedCodecWireFormat::Syslog = wire_format {
        if !encoding_rules.is_empty() {
            return Err(Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: "SYSLOG codecs do not support ENCODE field rules".to_string(),
            }));
        }
        HashMap::new()
    } else {
        ensure_supported_codec_encoding_rules(domain, identifier, schema, encoding_rules)?
    };
    match wire_format {
        ResolvedCodecWireFormat::Syslog => ensure_syslog_field_contract(domain, identifier, schema),
        ResolvedCodecWireFormat::Json(json) => ensure_wire_field_set_matches(
            domain,
            identifier,
            &json.fields,
            schema,
            "json",
            &field_encodings,
        ),
        ResolvedCodecWireFormat::Cbor(cbor) => ensure_wire_field_set_matches(
            domain,
            identifier,
            &cbor.fields,
            schema,
            "cbor",
            &field_encodings,
        ),
        ResolvedCodecWireFormat::Avro(avro) => ensure_wire_field_set_matches(
            domain,
            identifier,
            &avro.fields,
            schema,
            "avro",
            &field_encodings,
        ),
        ResolvedCodecWireFormat::JaqNative {
            transformations, ..
        } => {
            if !transformations.has_any() {
                Err(Report::new(RegistryError::InvalidModel {
                    domain: domain.as_str().to_string(),
                    identifier: identifier.as_str().to_string(),
                    reason: "JAQ-native codec must declare a JAQ transformation".to_string(),
                }))
            } else {
                ensure_codec_jaq_programs(domain, identifier, transformations)
            }
        }
        ResolvedCodecWireFormat::Protobuf(config) => {
            if !config.transformations.has_any() {
                Err(Report::new(RegistryError::InvalidModel {
                    domain: domain.as_str().to_string(),
                    identifier: identifier.as_str().to_string(),
                    reason: "protobuf codec must declare a JAQ transformation".to_string(),
                }))
            } else {
                ensure_codec_jaq_programs(domain, identifier, &config.transformations)
            }
        }
    }
}

fn ensure_codec_jaq_programs(
    domain: &DomainName,
    identifier: &ModelName,
    transformations: &CodecJaqTransformations,
) -> Result<(), Report<RegistryError>> {
    for (direction, program) in [
        ("ingestion", transformations.on_ingestion.as_deref()),
        ("emitting", transformations.on_emitting.as_deref()),
        (
            "emitting batch",
            transformations.on_emitting_batch.as_deref(),
        ),
    ] {
        if let Some(program) = program {
            CompiledJaqProgram::compile(program).map_err(|error| {
                error.change_context(RegistryError::InvalidCodecJaq {
                    domain: domain.clone(),
                    codec: identifier.clone(),
                    direction,
                })
            })?;
        }
    }
    Ok(())
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
) -> Result<HashMap<FieldName, CodecEncoding>, Report<RegistryError>> {
    let mut field_encodings = HashMap::new();
    for rule in encoding_rules {
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

        // A rule on a list field governs its innermost scalar elements.
        let expected = match rule.encoding {
            CodecEncoding::Rfc3339 => ParseAsType::Datetime,
            CodecEncoding::Base64 => ParseAsType::Bytes,
        };
        if *schema_field.ty.innermost_element() != expected {
            return Err(Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "codec encoding rule field '{}' must be {expected} or a list of {expected} \
                     elements, found {}",
                    rule.field.as_str(),
                    schema_field.ty
                ),
            }));
        }

        if field_encodings
            .insert(rule.field.clone(), rule.encoding)
            .is_some()
        {
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
    Ok(field_encodings)
}

/// Why one wire field cannot carry an internal field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WireBindingRefusal {
    /// The wire type holds another kind of value than the internal type.
    Type,
    /// The AVRO arrays nest to another depth than the internal `ARRAY` and `VEC` levels.
    Depth { wire: usize, internal: usize },
    /// The innermost internal values travel as text only under this rule, which the codec does
    /// not declare for the field.
    EncodingRequired(CodecEncoding),
    /// The codec declares this text rule for bytes the wire type carries natively.
    EncodingRefused(CodecEncoding),
}

impl WireBindingRefusal {
    /// The refusal as the reason of an incompatible schema relationship, naming the field and
    /// both types.
    fn reason(
        self,
        wire_kind: &str,
        field: &FieldName,
        wire: &dyn fmt::Display,
        internal: &ParseAsType,
    ) -> String {
        let field = field.as_str();
        match self {
            Self::Type => {
                format!(
                    "{wire_kind} field '{field}' type mismatch: wire {wire}, internal {internal}"
                )
            }
            Self::Depth {
                wire: wire_depth,
                internal: internal_depth,
            } => {
                let levels = if wire_depth == 1 { "level" } else { "levels" };
                format!(
                    "{wire_kind} field '{field}' array depth mismatch: wire {wire} has \
                     {wire_depth} array {levels}, internal {internal} has {internal_depth}"
                )
            }
            Self::EncodingRequired(encoding) => {
                let values = if internal.list_depth() == 0 {
                    "value"
                } else {
                    "elements"
                };
                format!(
                    "{wire_kind} field '{field}' needs ENCODE {field} AS {} for its {} {values}",
                    encoding.as_ref(),
                    internal.innermost_element()
                )
            }
            Self::EncodingRefused(encoding) => format!(
                "{wire_kind} field '{field}' carries {} natively, so ENCODE {field} AS {} does \
                 not apply",
                internal.innermost_element(),
                encoding.as_ref()
            ),
        }
    }
}

/// The internal types one format's wire type carries, and under which codec rule.
trait WireTypeBinding: fmt::Display {
    /// Why this wire type cannot carry an internal value of type `ty` whose field declares
    /// `encoding`, or `None` when it carries it exactly.
    fn refusal(
        &self,
        ty: &ParseAsType,
        encoding: Option<CodecEncoding>,
    ) -> Option<WireBindingRefusal>;
}

/// JSON's `array` is bare: an element is written as a scalar field of its type would be.
impl WireTypeBinding for JsonType {
    fn refusal(
        &self,
        ty: &ParseAsType,
        encoding: Option<CodecEncoding>,
    ) -> Option<WireBindingRefusal> {
        match self {
            Self::String => text_refusal(ty, encoding),
            Self::Number => refused_unless(ty_is_float(ty)),
            Self::Integer => refused_unless(ty_is_integer(ty)),
            Self::Boolean => refused_unless(*ty == ParseAsType::Bool),
            Self::Array => {
                if ty.list_depth() == 0 {
                    return Some(WireBindingRefusal::Type);
                }
                match ty.innermost_element() {
                    ParseAsType::Datetime => required_encoding(encoding, CodecEncoding::Rfc3339),
                    ParseAsType::Bytes => required_encoding(encoding, CodecEncoding::Base64),
                    _ => None,
                }
            }
        }
    }
}

/// CBOR's `array` is bare: an element is written as a scalar field of its type would be, bytes as
/// native byte strings.
impl WireTypeBinding for CborType {
    fn refusal(
        &self,
        ty: &ParseAsType,
        encoding: Option<CodecEncoding>,
    ) -> Option<WireBindingRefusal> {
        match self {
            Self::Text => match ty {
                ParseAsType::String => None,
                ParseAsType::Datetime => required_encoding(encoding, CodecEncoding::Rfc3339),
                _ => Some(WireBindingRefusal::Type),
            },
            Self::Bytes => native_bytes_refusal(ty, encoding),
            Self::Float => refused_unless(ty_is_float(ty)),
            Self::Integer => refused_unless(ty_is_integer(ty)),
            Self::Boolean => refused_unless(*ty == ParseAsType::Bool),
            Self::Array => {
                if ty.list_depth() == 0 {
                    return Some(WireBindingRefusal::Type);
                }
                match ty.innermost_element() {
                    ParseAsType::Datetime => required_encoding(encoding, CodecEncoding::Rfc3339),
                    element @ ParseAsType::Bytes => native_bytes_refusal(element, encoding),
                    _ => None,
                }
            }
        }
    }
}

/// An AVRO array names its item type, so it binds a list nested exactly as deep whose innermost
/// element its innermost item binds as a field of that type would.
impl WireTypeBinding for AvroType {
    fn refusal(
        &self,
        ty: &ParseAsType,
        encoding: Option<CodecEncoding>,
    ) -> Option<WireBindingRefusal> {
        let wire = self.array_depth();
        let internal = ty.list_depth();
        if wire != internal && wire > 0 && internal > 0 {
            return Some(WireBindingRefusal::Depth { wire, internal });
        }
        avro_item_refusal(self, ty, encoding)
    }
}

/// Binds an AVRO type to an internal type one level at a time: an array binds a list whose
/// elements its items bind, and a primitive type binds the scalar it names.
fn avro_item_refusal(
    wire: &AvroType,
    ty: &ParseAsType,
    encoding: Option<CodecEncoding>,
) -> Option<WireBindingRefusal> {
    match (wire, ty) {
        (
            AvroType::Array(item),
            ParseAsType::Array { element, .. } | ParseAsType::Vec { element },
        ) => avro_item_refusal(item, element, encoding),
        (AvroType::Boolean, ParseAsType::Bool)
        | (AvroType::Int, ParseAsType::I32)
        | (AvroType::Long, ParseAsType::I64)
        | (AvroType::Float, ParseAsType::F32)
        | (AvroType::Double, ParseAsType::F64) => None,
        (AvroType::String, element) => text_refusal(element, encoding),
        (AvroType::Bytes, element) => native_bytes_refusal(element, encoding),
        _ => Some(WireBindingRefusal::Type),
    }
}

/// A text wire type carries `STRING` as it is, `DATETIME` under `RFC3339` and `BYTES` under
/// `BASE64`.
fn text_refusal(ty: &ParseAsType, encoding: Option<CodecEncoding>) -> Option<WireBindingRefusal> {
    match ty {
        ParseAsType::String => None,
        ParseAsType::Datetime => required_encoding(encoding, CodecEncoding::Rfc3339),
        ParseAsType::Bytes => required_encoding(encoding, CodecEncoding::Base64),
        _ => Some(WireBindingRefusal::Type),
    }
}

/// A native byte type carries `BYTES` as its own octets, under no text rule.
fn native_bytes_refusal(
    ty: &ParseAsType,
    encoding: Option<CodecEncoding>,
) -> Option<WireBindingRefusal> {
    match (ty, encoding) {
        (ParseAsType::Bytes, None) => None,
        (ParseAsType::Bytes, Some(encoding)) => Some(WireBindingRefusal::EncodingRefused(encoding)),
        _ => Some(WireBindingRefusal::Type),
    }
}

fn required_encoding(
    declared: Option<CodecEncoding>,
    required: CodecEncoding,
) -> Option<WireBindingRefusal> {
    if declared == Some(required) {
        None
    } else {
        Some(WireBindingRefusal::EncodingRequired(required))
    }
}

fn refused_unless(binds: bool) -> Option<WireBindingRefusal> {
    if binds {
        None
    } else {
        Some(WireBindingRefusal::Type)
    }
}

fn ensure_wire_field_set_matches<T: WireTypeBinding>(
    domain: &DomainName,
    identifier: &ModelName,
    wire_fields: &[WireSchemaField<T>],
    schema: &CreateSchema,
    wire_kind: &str,
    field_encodings: &HashMap<FieldName, CodecEncoding>,
) -> Result<(), Report<RegistryError>> {
    let mut wire_fields_by_name = HashMap::with_capacity(wire_fields.len());
    for wire_field in wire_fields {
        wire_fields_by_name
            .entry(&wire_field.name)
            .or_insert(wire_field);
    }
    for schema_field in &schema.fields {
        let Some(wire_field) = wire_fields_by_name.get(&schema_field.name) else {
            return Err(Report::new(RegistryError::IncompatibleSchema {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "{wire_kind} wire schema is missing field '{}'",
                    schema_field.name.as_str()
                ),
            }));
        };

        let encoding = field_encodings.get(&schema_field.name).copied();
        if let Some(refusal) = wire_field.ty.refusal(&schema_field.ty, encoding) {
            return Err(Report::new(RegistryError::IncompatibleSchema {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: refusal.reason(
                    wire_kind,
                    &schema_field.name,
                    &wire_field.ty,
                    &schema_field.ty,
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

fn ty_is_float(ty: &ParseAsType) -> bool {
    matches!(ty, ParseAsType::F32 | ParseAsType::F64)
}

fn ty_is_integer(ty: &ParseAsType) -> bool {
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

    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_models::{
        EmitterEntity, SchemaField, SchemaName, SyslogCollectorEmitter, SyslogEmitter,
        WireSchemaField,
    };
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
                labels: Default::default(),
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

    #[test]
    fn codec_jaq_syntax_is_checked_before_model_commit_for_every_direction() {
        let domain = DomainName::parse("example").assured("example is a valid domain");
        let identifier =
            ModelName::parse("event_codec").assured("event_codec is a valid model name");
        let schema = CreateSchema {
            labels: Default::default(),
            name: named("event_schema"),
            fields: Vec::new(),
        };
        for (direction, transformations) in [
            (
                "ingestion",
                CodecJaqTransformations {
                    on_ingestion: Some("[".to_string()),
                    ..CodecJaqTransformations::default()
                },
            ),
            (
                "emitting",
                CodecJaqTransformations {
                    on_emitting: Some("[".to_string()),
                    ..CodecJaqTransformations::default()
                },
            ),
            (
                "emitting batch",
                CodecJaqTransformations {
                    on_emitting: Some(".".to_string()),
                    on_emitting_batch: Some("[".to_string()),
                    ..CodecJaqTransformations::default()
                },
            ),
        ] {
            let error = ensure_codec_schema_compatibility(
                &domain,
                &identifier,
                ResolvedCodecWireFormat::JaqNative {
                    format: nervix_models::CodecJaqFormat::Json,
                    transformations: &transformations,
                },
                &schema,
                &[],
            )
            .err()
            .verified("the malformed jaq program is rejected");
            assert!(
                matches!(error.current_context(), RegistryError::InvalidCodecJaq { direction: actual, .. } if *actual == direction)
            );
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
                        labels: Default::default(),
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
                        labels: Default::default(),
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
                            labels: Default::default(),
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
        emitter.connector = Box::new(EmitterEntity::Syslog(SyslogEmitter::Collector(
            SyslogCollectorEmitter {
                mode: nervix_models::SyslogCollectorPublishingMode::NoAck {
                    retry_policy: nervix_models::RetryPolicy {
                        backoff: "250ms".to_string(),
                        max_backoff: "30s".to_string(),
                    },
                },
                client: named("syslog_out"),
            },
        )));

        let error = registry
            .apply_batch(
                &domain,
                vec![
                    Model::Schema(CreateSchema {
                        labels: Default::default(),
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

    /// One wire field type of any declared format.
    #[derive(Debug, Clone, PartialEq, Eq)]
    enum WireCase {
        Json(JsonType),
        Cbor(CborType),
        Avro(AvroType),
    }

    #[derive(Debug)]
    struct CodecBindingCase {
        wire: WireCase,
        internal: ParseAsType,
        rules: Vec<CodecEncodingRule>,
    }

    impl CodecBindingCase {
        /// A field `value` of `internal` type, bound by `wire`, whose codec declares `encoding`.
        fn new(wire: WireCase, internal: ParseAsType, encoding: Option<CodecEncoding>) -> Self {
            let rules = encoding
                .into_iter()
                .map(|encoding| CodecEncodingRule {
                    field: named("value"),
                    encoding,
                })
                .collect();
            Self {
                wire,
                internal,
                rules,
            }
        }

        fn validate(&self) -> Result<(), Report<RegistryError>> {
            let schema = CreateSchema {
                labels: Default::default(),
                name: named("binary_schema"),
                fields: vec![SchemaField {
                    name: named("value"),
                    ty: self.internal.clone(),
                    optional: false,
                    sensitive: false,
                }],
            };
            let domain = named("example");
            let identifier = named("binary_codec");
            macro_rules! validate_binding {
                ($format:ident, $ty:expr) => {
                    ensure_codec_schema_compatibility(
                        &domain,
                        &identifier,
                        ResolvedCodecWireFormat::$format(
                            (&CreateWireSchema {
                                labels: Default::default(),
                                name: named("binary_wire"),
                                strictness: Default::default(),
                                fields: vec![WireSchemaField {
                                    name: named("value"),
                                    ty: $ty,
                                    optional: false,
                                }],
                            })
                                .into(),
                        ),
                        &schema,
                        &self.rules,
                    )
                };
            }
            match &self.wire {
                WireCase::Json(ty) => validate_binding!(Json, *ty),
                WireCase::Cbor(ty) => validate_binding!(Cbor, *ty),
                WireCase::Avro(ty) => validate_binding!(Avro, ty.clone()),
            }
        }
    }

    #[test]
    fn validates_every_json_and_avro_field_binding() {
        struct BindingRow {
            wire: WireCase,
            types: Vec<ParseAsType>,
            encoding: Option<CodecEncoding>,
        }
        use ParseAsType::{
            Bool, Bytes, Datetime, F32, F64, I8, I16, I32, I64, String, U8, U16, U32, U64,
        };
        use WireCase::{Avro, Json};
        let lists = vec![
            ParseAsType::Vec {
                element: Box::new(String),
            },
            ParseAsType::Array {
                element: Box::new(I64),
                len: std::num::NonZeroU32::MIN,
            },
        ];
        let rows = [
            BindingRow {
                wire: Json(JsonType::String),
                types: vec![String],
                encoding: None,
            },
            BindingRow {
                wire: Json(JsonType::String),
                types: vec![Datetime],
                encoding: Some(CodecEncoding::Rfc3339),
            },
            BindingRow {
                wire: Json(JsonType::String),
                types: vec![Bytes],
                encoding: Some(CodecEncoding::Base64),
            },
            BindingRow {
                wire: Json(JsonType::Integer),
                types: vec![U8, I8, U16, I16, U32, I32, U64, I64],
                encoding: None,
            },
            BindingRow {
                wire: Json(JsonType::Number),
                types: vec![F32, F64],
                encoding: None,
            },
            BindingRow {
                wire: Json(JsonType::Boolean),
                types: vec![Bool],
                encoding: None,
            },
            BindingRow {
                wire: Json(JsonType::Array),
                types: lists.clone(),
                encoding: None,
            },
            BindingRow {
                wire: Avro(AvroType::Boolean),
                types: vec![Bool],
                encoding: None,
            },
            BindingRow {
                wire: Avro(AvroType::Int),
                types: vec![I32],
                encoding: None,
            },
            BindingRow {
                wire: Avro(AvroType::Long),
                types: vec![I64],
                encoding: None,
            },
            BindingRow {
                wire: Avro(AvroType::Float),
                types: vec![F32],
                encoding: None,
            },
            BindingRow {
                wire: Avro(AvroType::Double),
                types: vec![F64],
                encoding: None,
            },
            BindingRow {
                wire: Avro(AvroType::String),
                types: vec![String],
                encoding: None,
            },
            BindingRow {
                wire: Avro(AvroType::String),
                types: vec![Datetime],
                encoding: Some(CodecEncoding::Rfc3339),
            },
            BindingRow {
                wire: Avro(AvroType::String),
                types: vec![Bytes],
                encoding: Some(CodecEncoding::Base64),
            },
            BindingRow {
                wire: Avro(AvroType::Bytes),
                types: vec![Bytes],
                encoding: None,
            },
            BindingRow {
                wire: Avro(AvroType::array_of(AvroType::String)),
                types: vec![lists[0].clone()],
                encoding: None,
            },
            BindingRow {
                wire: Avro(AvroType::array_of(AvroType::Long)),
                types: vec![lists[1].clone()],
                encoding: None,
            },
        ];
        let mut types = ParseAsType::scalar_variants().to_vec();
        types.extend(lists);
        let wires = [
            Json(JsonType::String),
            Json(JsonType::Integer),
            Json(JsonType::Number),
            Json(JsonType::Boolean),
            Json(JsonType::Array),
            Avro(AvroType::Boolean),
            Avro(AvroType::Int),
            Avro(AvroType::Long),
            Avro(AvroType::Float),
            Avro(AvroType::Double),
            Avro(AvroType::String),
            Avro(AvroType::Bytes),
            Avro(AvroType::array_of(AvroType::String)),
            Avro(AvroType::array_of(AvroType::Long)),
        ];
        for wire in &wires {
            for internal in &types {
                for encoding in [
                    None,
                    Some(CodecEncoding::Rfc3339),
                    Some(CodecEncoding::Base64),
                ] {
                    // The oracle is this finite public matrix: 18 rows, each with at most eight types.
                    let accepted = rows.iter().any(|row| {
                        row.wire == *wire
                            && row.encoding == encoding
                            && row.types.contains(internal)
                    });
                    let case = CodecBindingCase::new(wire.clone(), internal.clone(), encoding);
                    let result = case.validate();
                    assert_eq!(result.is_ok(), accepted, "{case:?}: {result:?}");
                    if let Err(error) = result {
                        let rendered = format!("{error:#}");
                        assert!(rendered.contains("binary_codec"), "{rendered}");
                        assert!(rendered.contains("value"), "{rendered}");
                    }
                }
            }
        }
    }

    #[test]
    fn base64_rules_reject_unknown_and_duplicate_fields() {
        for fields in [&["missing"][..], &["value", "value"][..]] {
            let case = CodecBindingCase {
                wire: WireCase::Json(JsonType::String),
                internal: ParseAsType::Bytes,
                rules: fields
                    .iter()
                    .map(|field| CodecEncodingRule {
                        field: named(field),
                        encoding: CodecEncoding::Base64,
                    })
                    .collect(),
            };
            let error = case
                .validate()
                .err()
                .assured("a rule cannot reference an absent field or declare the same field twice");
            assert!(matches!(
                error.current_context(),
                RegistryError::InvalidModel { .. }
            ));
            let rendered = format!("{error:#}");
            assert!(rendered.contains("binary_codec"), "{rendered}");
            assert!(rendered.contains(fields[0]), "{rendered}");
        }
    }

    /// The list `element` wrapped in `depth` levels, alternating `VEC` and three-element `ARRAY`.
    fn listed(element: &ParseAsType, depth: usize) -> ParseAsType {
        let mut internal = element.clone();
        for level in 0..depth {
            internal = if level % 2 == 0 {
                ParseAsType::Vec {
                    element: Box::new(internal),
                }
            } else {
                ParseAsType::Array {
                    element: Box::new(internal),
                    len: nonzero_ext::nonzero!(3u32),
                }
            };
        }
        internal
    }

    /// The AVRO `item` inside `depth` arrays.
    fn avro_array(item: &AvroType, depth: usize) -> AvroType {
        let mut wire = item.clone();
        for _ in 0..depth {
            wire = AvroType::array_of(wire);
        }
        wire
    }

    #[test]
    fn avro_array_items_bind_the_elements_their_scalar_fields_bind_at_every_depth() {
        let encodings = [
            None,
            Some(CodecEncoding::Rfc3339),
            Some(CodecEncoding::Base64),
        ];
        for item in AvroType::scalar_variants() {
            for element in ParseAsType::scalar_variants() {
                for encoding in encodings {
                    let scalar = CodecBindingCase::new(
                        WireCase::Avro(item.clone()),
                        element.clone(),
                        encoding,
                    )
                    .validate();
                    for depth in 1..=3 {
                        let case = CodecBindingCase::new(
                            WireCase::Avro(avro_array(item, depth)),
                            listed(element, depth),
                            encoding,
                        );
                        let result = case.validate();
                        assert_eq!(
                            result.is_ok(),
                            scalar.is_ok(),
                            "{case:?}: {result:?}, while the scalar binding gives {scalar:?}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn an_avro_array_nested_to_another_depth_than_its_list_is_refused() {
        for (wire, internal, reason) in [
            (
                avro_array(&AvroType::Long, 1),
                ParseAsType::Array {
                    element: Box::new(ParseAsType::Array {
                        element: Box::new(ParseAsType::I64),
                        len: nonzero_ext::nonzero!(3u32),
                    }),
                    len: nonzero_ext::nonzero!(2u32),
                },
                "avro field 'value' array depth mismatch: wire array<long> has 1 array level, \
                 internal ARRAY<ARRAY<I64, 3>, 2> has 2",
            ),
            (
                avro_array(&AvroType::Long, 2),
                listed(&ParseAsType::I64, 1),
                "avro field 'value' array depth mismatch: wire array<array<long>> has 2 array \
                 levels, internal VEC<I64> has 1",
            ),
            (
                avro_array(&AvroType::Long, 1),
                ParseAsType::I64,
                "avro field 'value' type mismatch: wire array<long>, internal I64",
            ),
            (
                AvroType::Long,
                listed(&ParseAsType::I64, 1),
                "avro field 'value' type mismatch: wire long, internal VEC<I64>",
            ),
            (
                avro_array(&AvroType::Long, 1),
                listed(&ParseAsType::I32, 1),
                "avro field 'value' type mismatch: wire array<long>, internal VEC<I32>",
            ),
        ] {
            let error = CodecBindingCase::new(WireCase::Avro(wire), internal, None)
                .validate()
                .err()
                .assured("each case pairs an AVRO array with a list it cannot carry");
            assert!(
                matches!(
                    error.current_context(),
                    RegistryError::IncompatibleSchema { identifier, .. } if identifier == "binary_codec"
                ),
                "{error:#}"
            );
            let rendered = format!("{error:#}");
            assert!(rendered.contains(reason), "{rendered}");
        }
    }

    #[test]
    fn list_elements_bind_through_the_fields_encode_rule_in_every_format() {
        use CodecEncoding::{Base64, Rfc3339};
        use ParseAsType::{Bytes, Datetime};
        let nested_bytes = listed(&Bytes, 2);
        let cases = [
            (
                WireCase::Json(JsonType::Array),
                listed(&Datetime, 1),
                None,
                Err("json field 'value' needs ENCODE value AS RFC3339 for its DATETIME elements"),
            ),
            (
                WireCase::Json(JsonType::Array),
                listed(&Datetime, 1),
                Some(Rfc3339),
                Ok(()),
            ),
            (
                WireCase::Json(JsonType::Array),
                nested_bytes.clone(),
                None,
                Err("json field 'value' needs ENCODE value AS BASE64 for its BYTES elements"),
            ),
            (
                WireCase::Json(JsonType::Array),
                nested_bytes.clone(),
                Some(Base64),
                Ok(()),
            ),
            (
                WireCase::Json(JsonType::String),
                Datetime,
                None,
                Err("json field 'value' needs ENCODE value AS RFC3339 for its DATETIME value"),
            ),
            (
                WireCase::Cbor(CborType::Array),
                listed(&Datetime, 1),
                None,
                Err("cbor field 'value' needs ENCODE value AS RFC3339 for its DATETIME elements"),
            ),
            (
                WireCase::Cbor(CborType::Array),
                listed(&Datetime, 1),
                Some(Rfc3339),
                Ok(()),
            ),
            (
                WireCase::Cbor(CborType::Array),
                nested_bytes.clone(),
                None,
                Ok(()),
            ),
            (
                WireCase::Cbor(CborType::Array),
                nested_bytes.clone(),
                Some(Base64),
                Err(
                    "cbor field 'value' carries BYTES natively, so ENCODE value AS BASE64 does \
                     not apply",
                ),
            ),
            (
                WireCase::Cbor(CborType::Bytes),
                Bytes,
                Some(Base64),
                Err(
                    "cbor field 'value' carries BYTES natively, so ENCODE value AS BASE64 does \
                     not apply",
                ),
            ),
            (
                WireCase::Avro(avro_array(&AvroType::String, 1)),
                listed(&Datetime, 1),
                None,
                Err("avro field 'value' needs ENCODE value AS RFC3339 for its DATETIME elements"),
            ),
            (
                WireCase::Avro(avro_array(&AvroType::String, 1)),
                listed(&Datetime, 1),
                Some(Rfc3339),
                Ok(()),
            ),
            (
                WireCase::Avro(avro_array(&AvroType::String, 2)),
                nested_bytes.clone(),
                Some(Base64),
                Ok(()),
            ),
            (
                WireCase::Avro(avro_array(&AvroType::Bytes, 2)),
                nested_bytes.clone(),
                None,
                Ok(()),
            ),
            (
                WireCase::Avro(avro_array(&AvroType::Bytes, 2)),
                nested_bytes,
                Some(Base64),
                Err(
                    "avro field 'value' carries BYTES natively, so ENCODE value AS BASE64 does \
                     not apply",
                ),
            ),
        ];
        for (wire, internal, encoding, expected) in cases {
            let case = CodecBindingCase::new(wire, internal, encoding);
            let result = case.validate();
            match expected {
                Ok(()) => {
                    result.unwrap_or_else(|error| panic!("{case:?} must bind: {error:#}"));
                }
                Err(reason) => {
                    let error = result
                        .err()
                        .assured("this case pairs a list element with a rule it cannot use");
                    assert!(
                        matches!(
                            error.current_context(),
                            RegistryError::IncompatibleSchema { .. }
                        ),
                        "{case:?}: {error:#}"
                    );
                    let rendered = format!("{error:#}");
                    assert!(rendered.contains(reason), "{case:?}: {rendered}");
                }
            }
        }
    }

    #[test]
    fn a_rule_on_a_list_field_needs_its_innermost_elements_to_be_the_rules_type() {
        for (internal, encoding, reason) in [
            (
                listed(&ParseAsType::String, 1),
                CodecEncoding::Rfc3339,
                "codec encoding rule field 'value' must be DATETIME or a list of DATETIME \
                 elements, found VEC<STRING>",
            ),
            (
                listed(&ParseAsType::Datetime, 2),
                CodecEncoding::Base64,
                "codec encoding rule field 'value' must be BYTES or a list of BYTES elements, \
                 found ARRAY<VEC<DATETIME>, 3>",
            ),
        ] {
            let error =
                CodecBindingCase::new(WireCase::Json(JsonType::Array), internal, Some(encoding))
                    .validate()
                    .err()
                    .assured("the rule names a field whose elements are of another type");
            assert!(
                matches!(error.current_context(), RegistryError::InvalidModel { .. }),
                "{error:#}"
            );
            let rendered = format!("{error:#}");
            assert!(rendered.contains(reason), "{rendered}");
        }
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
    #[case::requires_base64_for_json_string_bytes(
        CodecTypeCase::Json {
            internal: ParseAsType::Bytes,
            wire: JsonType::String,
            rfc3339_field: None,
        },
        CodecTypeExpectation::IncompatibleSchema
    )]
    #[case::accepts_avro_bytes_for_internal_bytes(
        CodecTypeCase::Avro {
            internal: ParseAsType::Bytes,
            wire: nervix_models::AvroType::Bytes,
        },
        CodecTypeExpectation::Accepted
    )]
    #[case::rejects_avro_bytes_for_internal_text(
        CodecTypeCase::Avro {
            internal: ParseAsType::String,
            wire: nervix_models::AvroType::Bytes,
        },
        CodecTypeExpectation::IncompatibleSchema
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
                        labels: Default::default(),
                        name: SchemaName::parse("event_schema").expect("valid identifier"),
                        fields: vec![SchemaField {
                            name: FieldName::parse("value").expect("valid identifier"),
                            ty: nervix_models::ParseAsType::String,
                            optional: false,
                            sensitive: false,
                        }],
                    }),
                    Model::WireJsonSchema(CreateWireSchema {
                        labels: Default::default(),
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

    /// Every codec the schemaful codec properties generate is one this validation accepts, so the
    /// properties cover only codecs a domain can declare and every shape a domain can declare is
    /// one they may generate.
    #[test]
    fn generated_schemaful_codecs_are_accepted() {
        use nervix_arbitrary::{Arbitrary, Domain};

        use crate::runtime_schema::codec_properties::CodecCase;

        let domain = DomainName::parse("generated").assured("a literal domain name");
        for seed in 0..=u8::MAX {
            let mut bytes = Vec::with_capacity(1024);
            for index in 0..1024_usize {
                let spread = index
                    .checked_mul(31)
                    .and_then(|spread| spread.checked_add(usize::from(seed)))
                    .assured("a small index times a small factor fits in usize");
                bytes.push(u8::try_from(spread % 256).assured("a remainder of 256 is one byte"));
            }
            let mut arbitrary = Arbitrary::new(&bytes, Domain::Vocabulary);
            let case = CodecCase::new(&mut arbitrary);
            let identifier = ModelName::from(&case.model.name);
            ensure_codec_schema_compatibility(
                &domain,
                &identifier,
                case.wire.resolved(),
                &case.schema.model,
                &case.model.encoding_rules,
            )
            .unwrap_or_else(|refused| {
                panic!("seed {seed} generated a codec the registry refuses: {refused:#}")
            });
        }
    }
}
