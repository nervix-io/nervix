//! Browser draft and semantic conversion for codec creation.
//!
//! Layer: edges.
//!
//! - **Owns.** Incomplete format, direction, transformation, encoding, and reference controls.
//! - **Depends on.** Current codec Models and the create dialog's selected references.
//! - **Must not know.** Codec compilation, registry contents, or resource installation.

use std::collections::BTreeSet;

use error_stack::Report;
use nervix_models::{
    CodecEncoding, CodecEncodingRule, CodecJaqFormat, CodecJaqTransformations, CodecName,
    CodecProtobufConfig, CodecWireFormat, CreateCodec, FieldName, ModelKind, NodeRef,
    RequestedResourceVersion, SchemaName, WireSchemaName,
};
use thiserror::Error;

use super::{
    SelectedReference,
    resource_binding_draft::{ResourceBindingDraft, ResourceDraftError},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CodecFormatKind {
    WireJson,
    WireCbor,
    WireAvro,
    Syslog,
    JaqJson,
    JaqYaml,
    JaqToml,
    JaqXml,
    JaqCbor,
    Protobuf,
}

impl CodecFormatKind {
    pub(super) const ALL: [Self; 10] = [
        Self::WireJson,
        Self::WireCbor,
        Self::WireAvro,
        Self::Syslog,
        Self::JaqJson,
        Self::JaqYaml,
        Self::JaqToml,
        Self::JaqXml,
        Self::JaqCbor,
        Self::Protobuf,
    ];

    pub(super) fn key(self) -> &'static str {
        match self {
            Self::WireJson => "wire-json",
            Self::WireCbor => "wire-cbor",
            Self::WireAvro => "wire-avro",
            Self::Syslog => "syslog",
            Self::JaqJson => "json",
            Self::JaqYaml => "yaml",
            Self::JaqToml => "toml",
            Self::JaqXml => "xml",
            Self::JaqCbor => "cbor",
            Self::Protobuf => "protobuf",
        }
    }

    pub(super) fn label(self) -> &'static str {
        match self {
            Self::WireJson => "Wire JSON",
            Self::WireCbor => "Wire CBOR",
            Self::WireAvro => "Wire AVRO",
            Self::Syslog => "SYSLOG",
            Self::JaqJson => "JSON + jaq",
            Self::JaqYaml => "YAML + jaq",
            Self::JaqToml => "TOML + jaq",
            Self::JaqXml => "XML + jaq",
            Self::JaqCbor => "CBOR + jaq",
            Self::Protobuf => "Protobuf + jaq",
        }
    }

    fn jaq_format(self) -> Option<CodecJaqFormat> {
        match self {
            Self::JaqJson => Some(CodecJaqFormat::Json),
            Self::JaqYaml => Some(CodecJaqFormat::Yaml),
            Self::JaqToml => Some(CodecJaqFormat::Toml),
            Self::JaqXml => Some(CodecJaqFormat::Xml),
            Self::JaqCbor => Some(CodecJaqFormat::Cbor),
            _ => None,
        }
    }

    pub(super) fn wire_schema_kind(self) -> Option<ModelKind> {
        match self {
            Self::WireJson => Some(ModelKind::WireJsonSchema),
            Self::WireCbor => Some(ModelKind::WireCborSchema),
            Self::WireAvro => Some(ModelKind::WireAvroSchema),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct JaqTransformDraft {
    pub(super) ingestion: Option<String>,
    pub(super) emitting: Option<String>,
    pub(super) emitting_batch: Option<String>,
}

impl JaqTransformDraft {
    fn build(&self) -> error_stack::Result<CodecJaqTransformations, CodecDraftError> {
        if self.ingestion.is_none() && self.emitting.is_none() {
            return Err(Report::new(CodecDraftError::DirectionRequired));
        }
        for (label, program) in [
            ("ingestion", self.ingestion.as_deref()),
            ("emitting", self.emitting.as_deref()),
            ("emitting batch", self.emitting_batch.as_deref()),
        ] {
            if program.is_some_and(|program| program.trim().is_empty()) {
                return Err(Report::new(CodecDraftError::ProgramRequired {
                    direction: label,
                }));
            }
        }
        if self.emitting_batch.is_some() && self.emitting.is_none() {
            return Err(Report::new(CodecDraftError::BatchNeedsEmitting));
        }
        Ok(CodecJaqTransformations {
            on_ingestion: self.ingestion.clone(),
            on_emitting: self.emitting.clone(),
            on_emitting_batch: self.emitting_batch.clone(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum CodecFormatDraft {
    Wire {
        kind: CodecFormatKind,
        schema: Option<SelectedReference<WireSchemaName>>,
    },
    Syslog,
    Jaq {
        kind: CodecFormatKind,
        transformations: JaqTransformDraft,
    },
    Protobuf {
        binding: ResourceBindingDraft,
        message: String,
        batch_message: String,
        transformations: JaqTransformDraft,
    },
}

impl CodecFormatDraft {
    pub(super) fn kind(&self) -> CodecFormatKind {
        match self {
            Self::Wire { kind, .. } | Self::Jaq { kind, .. } => *kind,
            Self::Syslog => CodecFormatKind::Syslog,
            Self::Protobuf { .. } => CodecFormatKind::Protobuf,
        }
    }

    fn build(
        &self,
    ) -> error_stack::Result<CodecWireFormat<RequestedResourceVersion>, CodecDraftError> {
        match self {
            Self::Wire { kind, schema } => {
                let Some(schema) = schema else {
                    return Err(Report::new(CodecDraftError::WireSchemaRequired));
                };
                let schema = schema
                    .current_name()
                    .ok_or_else(|| Report::new(CodecDraftError::WireSchemaChanged))?
                    .clone();
                Ok(match kind {
                    CodecFormatKind::WireJson => CodecWireFormat::Json {
                        wire_schema: schema,
                    },
                    CodecFormatKind::WireCbor => CodecWireFormat::Cbor {
                        wire_schema: schema,
                    },
                    CodecFormatKind::WireAvro => CodecWireFormat::Avro {
                        wire_schema: schema,
                    },
                    _ => return Err(Report::new(CodecDraftError::FormatRequired)),
                })
            }
            Self::Syslog => Ok(CodecWireFormat::Syslog),
            Self::Jaq {
                kind,
                transformations,
            } => {
                let format = kind
                    .jaq_format()
                    .ok_or_else(|| Report::new(CodecDraftError::FormatRequired))?;
                Ok(CodecWireFormat::JaqNative {
                    format,
                    transformations: transformations.build()?,
                })
            }
            Self::Protobuf {
                binding,
                message,
                batch_message,
                transformations,
            } => {
                let binding = binding.build().map_err(|error| {
                    let context = CodecDraftError::Resource(error.current_context().clone());
                    error.change_context(context)
                })?;
                if message.trim().is_empty() {
                    return Err(Report::new(CodecDraftError::MessageRequired));
                }
                let transformations = transformations.build()?;
                if transformations.on_emitting_batch.is_some() && batch_message.trim().is_empty() {
                    return Err(Report::new(CodecDraftError::BatchMessageRequired));
                }
                Ok(CodecWireFormat::Protobuf(CodecProtobufConfig {
                    resource: binding.resource,
                    resource_version: binding.version,
                    config: binding.config,
                    message: message.clone(),
                    batch_message: if batch_message.is_empty() {
                        None
                    } else {
                        Some(batch_message.clone())
                    },
                    transformations,
                }))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct EncodingRuleDraft {
    pub(super) field: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct CodecDraft {
    pub(super) name: String,
    pub(super) if_not_exists: bool,
    pub(super) schema: Option<SelectedReference<SchemaName>>,
    pub(super) format: Option<CodecFormatDraft>,
    pub(super) encoding_rules: Vec<EncodingRuleDraft>,
}

impl CodecDraft {
    pub(super) fn set_format(&mut self, kind: CodecFormatKind) {
        if self
            .format
            .as_ref()
            .is_some_and(|format| format.kind() == kind)
        {
            return;
        }
        self.format = Some(match kind {
            CodecFormatKind::WireJson | CodecFormatKind::WireCbor | CodecFormatKind::WireAvro => {
                CodecFormatDraft::Wire { kind, schema: None }
            }
            CodecFormatKind::Syslog => CodecFormatDraft::Syslog,
            CodecFormatKind::Protobuf => CodecFormatDraft::Protobuf {
                binding: ResourceBindingDraft::default(),
                message: String::new(),
                batch_message: String::new(),
                transformations: JaqTransformDraft::default(),
            },
            _ => CodecFormatDraft::Jaq {
                kind,
                transformations: JaqTransformDraft::default(),
            },
        });
        if kind == CodecFormatKind::Syslog {
            self.encoding_rules.clear();
        }
    }

    pub(super) fn select_schema(&mut self, node: &NodeRef) {
        if node.kind == ModelKind::Schema {
            self.schema = Some(SelectedReference::chosen(SchemaName::from(
                &node.identifier,
            )));
        }
    }

    pub(super) fn select_wire_schema(&mut self, node: &NodeRef) {
        if let Some(CodecFormatDraft::Wire { kind, schema }) = &mut self.format
            && kind.wire_schema_kind() == Some(node.kind)
        {
            *schema = Some(SelectedReference::chosen(WireSchemaName::from(
                &node.identifier,
            )));
        }
    }

    pub(super) fn invalidate_references(&mut self) {
        if let Some(schema) = &mut self.schema {
            schema.invalidate();
        }
        match &mut self.format {
            Some(CodecFormatDraft::Wire {
                schema: Some(schema),
                ..
            }) => schema.invalidate(),
            Some(CodecFormatDraft::Protobuf { binding, .. }) => binding.invalidate(),
            _ => {}
        }
    }

    pub(super) fn binding(&self) -> Option<&ResourceBindingDraft> {
        match &self.format {
            Some(CodecFormatDraft::Protobuf { binding, .. }) => Some(binding),
            _ => None,
        }
    }

    pub(super) fn binding_mut(&mut self) -> Option<&mut ResourceBindingDraft> {
        match &mut self.format {
            Some(CodecFormatDraft::Protobuf { binding, .. }) => Some(binding),
            _ => None,
        }
    }

    pub(super) fn transformations_mut(&mut self) -> Option<&mut JaqTransformDraft> {
        match &mut self.format {
            Some(CodecFormatDraft::Jaq {
                transformations, ..
            })
            | Some(CodecFormatDraft::Protobuf {
                transformations, ..
            }) => Some(transformations),
            _ => None,
        }
    }

    pub(super) fn transformations(&self) -> Option<&JaqTransformDraft> {
        match &self.format {
            Some(CodecFormatDraft::Jaq {
                transformations, ..
            })
            | Some(CodecFormatDraft::Protobuf {
                transformations, ..
            }) => Some(transformations),
            _ => None,
        }
    }

    pub(super) fn build(
        &self,
    ) -> error_stack::Result<CreateCodec<RequestedResourceVersion>, CodecDraftError> {
        let name =
            CodecName::parse(self.name.trim()).map_err(|_| Report::new(CodecDraftError::Name))?;
        let schema = self
            .schema
            .as_ref()
            .ok_or_else(|| Report::new(CodecDraftError::SchemaRequired))?
            .current_name()
            .ok_or_else(|| Report::new(CodecDraftError::SchemaChanged))?
            .clone();
        let wire_format = self
            .format
            .as_ref()
            .ok_or_else(|| Report::new(CodecDraftError::FormatRequired))?
            .build()?;
        if matches!(wire_format, CodecWireFormat::Syslog) && !self.encoding_rules.is_empty() {
            return Err(Report::new(CodecDraftError::SyslogEncoding));
        }
        let mut fields = BTreeSet::new();
        let mut encoding_rules = Vec::new();
        for (index, rule) in self.encoding_rules.iter().enumerate() {
            let field = FieldName::parse(rule.field.trim())
                .map_err(|_| Report::new(CodecDraftError::EncodingField { entry: index + 1 }))?;
            if !fields.insert(field.clone()) {
                return Err(Report::new(CodecDraftError::DuplicateEncodingField {
                    field,
                }));
            }
            encoding_rules.push(CodecEncodingRule {
                field,
                encoding: CodecEncoding::Rfc3339,
            });
        }
        Ok(CreateCodec {
            name,
            wire_format,
            schema,
            encoding_rules,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(super) enum CodecDraftError {
    #[error("Codec name is invalid")]
    Name,
    #[error("Choose an internal schema")]
    SchemaRequired,
    #[error("The selected internal schema belongs to a changed context; select it again")]
    SchemaChanged,
    #[error("Choose a wire format")]
    FormatRequired,
    #[error("Choose a wire schema of the selected format")]
    WireSchemaRequired,
    #[error("The selected wire schema belongs to a changed context; select it again")]
    WireSchemaChanged,
    #[error("Choose ingestion or emitting for the jaq program")]
    DirectionRequired,
    #[error("The {direction} jaq program is required")]
    ProgramRequired { direction: &'static str },
    #[error("Batch transformation requires an emitting transformation")]
    BatchNeedsEmitting,
    #[error("Protobuf message is required")]
    MessageRequired,
    #[error("Protobuf batch message is required for a batch transformation")]
    BatchMessageRequired,
    #[error("{0}")]
    Resource(#[from] ResourceDraftError),
    #[error("Encoding rule {entry} needs a field name")]
    EncodingField { entry: usize },
    #[error("Field {field} has more than one encoding rule")]
    DuplicateEncodingField { field: FieldName },
    #[error("SYSLOG does not support field encoding rules")]
    SyslogEncoding,
}

#[cfg(test)]
mod tests {
    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_models::{
        CreateStatement, Model, ModelKind, ModelName, NodeRef, RequestedResourceVersion, Statement,
    };
    use nervix_nspl::client_statement::{ClientStatement, parse_client_statement};

    use super::{
        CodecDraft, CodecDraftError, CodecFormatDraft, CodecFormatKind, EncodingRuleDraft,
        JaqTransformDraft,
    };

    fn node(kind: ModelKind, name: &str) -> NodeRef {
        NodeRef::new(kind, ModelName::parse(name).assured("test names are valid"))
    }

    fn draft(kind: CodecFormatKind) -> CodecDraft {
        let mut draft = CodecDraft {
            name: "visual_codec".to_string(),
            ..CodecDraft::default()
        };
        draft.select_schema(&node(ModelKind::Schema, "payload"));
        draft.set_format(kind);
        draft
    }

    fn round_trip(draft: &CodecDraft) -> String {
        let codec = draft
            .build()
            .assured("the test codec has all required fields");
        let statement = Statement::Create(CreateStatement::new(
            Box::new(Model::<RequestedResourceVersion>::Codec(codec)),
            draft.if_not_exists,
        ));
        let source = statement
            .to_canonical_nspl()
            .assured("the completed codec renders");
        assert_eq!(
            parse_client_statement(&source).assured("canonical codec must parse"),
            ClientStatement::Server(statement),
        );
        source
    }

    #[test]
    fn every_schemaful_and_fixed_codec_format_round_trips_with_its_exact_wire_kind() {
        for (kind, model_kind, clause) in [
            (
                CodecFormatKind::WireJson,
                ModelKind::WireJsonSchema,
                "WIRE JSON",
            ),
            (
                CodecFormatKind::WireCbor,
                ModelKind::WireCborSchema,
                "WIRE CBOR",
            ),
            (
                CodecFormatKind::WireAvro,
                ModelKind::WireAvroSchema,
                "WIRE AVRO",
            ),
        ] {
            let mut draft = draft(kind);
            draft.select_wire_schema(&node(ModelKind::Schema, "wire_payload"));
            assert_eq!(
                draft
                    .build()
                    .err()
                    .map(|error| error.current_context().clone()),
                Some(CodecDraftError::WireSchemaRequired)
            );
            draft.select_wire_schema(&node(model_kind, "wire_payload"));
            draft.if_not_exists = true;
            let source = round_trip(&draft);
            assert!(source.contains(&format!("FROM {clause} SCHEMA wire_payload")));
        }
        let source = round_trip(&draft(CodecFormatKind::Syslog));
        assert!(source.contains("FROM SYSLOG"));
    }

    #[test]
    fn all_native_formats_keep_jaq_direction_and_batch_program_text() {
        for kind in [
            CodecFormatKind::JaqJson,
            CodecFormatKind::JaqYaml,
            CodecFormatKind::JaqToml,
            CodecFormatKind::JaqXml,
            CodecFormatKind::JaqCbor,
        ] {
            let mut draft = draft(kind);
            if let Some(CodecFormatDraft::Jaq {
                transformations, ..
            }) = &mut draft.format
            {
                *transformations = JaqTransformDraft {
                    ingestion: Some("{message: .message, quote: \"a'b\"}".to_string()),
                    emitting: Some("{message: .message}".to_string()),
                    emitting_batch: Some("{records: .}".to_string()),
                };
            }
            let source = round_trip(&draft);
            assert!(source.contains("ON INGESTION"));
            assert!(source.contains("ON EMITTING BATCH"));
            assert!(source.contains("a'b"));
        }
    }

    #[test]
    fn protobuf_keeps_resource_latest_compiler_inputs_and_batch_message() {
        let mut draft = draft(CodecFormatKind::Protobuf);
        if let Some(CodecFormatDraft::Protobuf {
            binding,
            message,
            batch_message,
            transformations,
        }) = &mut draft.format
        {
            binding.select_resource(
                nervix_models::ResourceName::parse("proto_bundle").assured("valid name"),
            );
            binding.select_version(RequestedResourceVersion::Latest);
            binding.file = "bundle.proto".to_string();
            binding.include = "imports".to_string();
            *message = "pkg.Payload".to_string();
            *batch_message = "pkg.Batch".to_string();
            transformations.ingestion = Some(".".to_string());
            transformations.emitting = Some(".".to_string());
            transformations.emitting_batch = Some("{rows: .}".to_string());
        }
        let source = round_trip(&draft);
        assert!(source.contains("USING RESOURCE proto_bundle VERSION LATEST"));
        assert!(source.contains("'file' = 'bundle.proto'"));
        assert!(source.contains("BATCH MESSAGE 'pkg.Batch'"));
        if let Some(CodecFormatDraft::Protobuf { binding, .. }) = &mut draft.format {
            binding.select_version(RequestedResourceVersion::Number(7));
        }
        draft.encoding_rules.push(EncodingRuleDraft {
            field: "created_at".to_string(),
        });
        let numbered = round_trip(&draft);
        assert!(numbered.contains("USING RESOURCE proto_bundle VERSION 7"));
        assert!(numbered.contains("created_at AS RFC3339"));
    }

    #[test]
    fn incomplete_directions_and_changed_references_are_rejected_locally() {
        let mut draft = draft(CodecFormatKind::JaqJson);
        assert_eq!(
            draft
                .build()
                .err()
                .map(|error| error.current_context().clone()),
            Some(CodecDraftError::DirectionRequired)
        );
        draft
            .transformations_mut()
            .verified("the selected format is jaq")
            .ingestion = Some("".to_string());
        assert_eq!(
            draft
                .build()
                .err()
                .map(|error| error.current_context().clone()),
            Some(CodecDraftError::ProgramRequired {
                direction: "ingestion"
            })
        );
        draft
            .transformations_mut()
            .verified("the selected format is jaq")
            .ingestion = Some(".".to_string());
        draft.invalidate_references();
        assert_eq!(
            draft
                .build()
                .err()
                .map(|error| error.current_context().clone()),
            Some(CodecDraftError::SchemaChanged)
        );
    }
}
