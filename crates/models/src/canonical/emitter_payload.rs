//! Effective emitter payload presentation.
//!
//! Layer: vocabulary.
//! - **Owns.** Typed format and container inspection from the current emitter and codec Models.
//! - **Depends on.** Entity payload declarations, codec formats and batching semantics.
//! - **Must not know.** Registry storage, codec execution, driver framing or runtime values.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary presentation declared above")
)]

use std::fmt::{Display, Formatter};

use crate::{
    CodecBatchContainer, CodecJaqFormat, CodecName, CodecWireFormat, CreateCodec, CreateEmitter,
    EmitterBody, EmitterPayloadFormat,
};

/// The completed container, independent of any record values or executable transformations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EmitterPayloadContainer {
    Record,
    Array,
    Table,
    Root,
    SyslogFrame,
    ProtobufMessage(String),
    TransformedValue,
    Undeclared,
}

/// The format selected by a codec, or the native payload declared by an entity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EmitterPayloadPresentation {
    Codec {
        format: String,
        container: EmitterPayloadContainer,
    },
    Native(EmitterPayloadFormat),
    WithoutBody,
}

impl CreateEmitter {
    /// Resolves inspection through the same coherent model view that supplied this emitter.
    /// A missing codec remains missing; inspection never invents a payload format.
    pub fn payload_presentation<'a>(
        &self,
        codec: impl FnOnce(&CodecName) -> Option<&'a CreateCodec>,
    ) -> Option<EmitterPayloadPresentation> {
        Some(match &self.body {
            EmitterBody::Codec { codec: name } => {
                EmitterPayloadPresentation::codec(&codec(name)?.wire_format, self.batch.is_some())
            }
            EmitterBody::WithoutBody => EmitterPayloadPresentation::WithoutBody,
            EmitterBody::Values | EmitterBody::Client => {
                EmitterPayloadPresentation::Native(self.connector.capabilities().payload)
            }
        })
    }
}

impl EmitterPayloadPresentation {
    fn codec(format: &CodecWireFormat, batched: bool) -> Self {
        let name = match format {
            CodecWireFormat::Json { .. } => "JSON",
            CodecWireFormat::Cbor { .. } => "CBOR",
            CodecWireFormat::Avro { .. } => "AVRO",
            CodecWireFormat::Syslog => "SYSLOG",
            CodecWireFormat::JaqNative { format, .. } => format.as_ref(),
            CodecWireFormat::Protobuf(_) => "PROTOBUF",
        };
        let container = if !batched {
            match format {
                CodecWireFormat::Protobuf(config) => {
                    EmitterPayloadContainer::ProtobufMessage(config.message.clone())
                }
                _ => EmitterPayloadContainer::Record,
            }
        } else {
            match format.batch_container() {
                CodecBatchContainer::Undeclared => EmitterPayloadContainer::Undeclared,
                container if container.transformation().is_some() => {
                    EmitterPayloadContainer::TransformedValue
                }
                CodecBatchContainer::ProtobufMessage { message, .. } => {
                    EmitterPayloadContainer::ProtobufMessage(message.to_owned())
                }
                CodecBatchContainer::Format { .. } => match format {
                    CodecWireFormat::JaqNative {
                        format: CodecJaqFormat::Toml,
                        ..
                    } => EmitterPayloadContainer::Table,
                    CodecWireFormat::JaqNative {
                        format: CodecJaqFormat::Xml,
                        ..
                    } => EmitterPayloadContainer::Root,
                    CodecWireFormat::Syslog => EmitterPayloadContainer::SyslogFrame,
                    _ => EmitterPayloadContainer::Array,
                },
            }
        };
        Self::Codec {
            format: name.to_owned(),
            container,
        }
    }
}

impl Display for EmitterPayloadPresentation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WithoutBody => f.write_str("no body"),
            Self::Native(format) => Display::fmt(format, f),
            Self::Codec { format, container } => {
                write!(f, "{format} ")?;
                match container {
                    EmitterPayloadContainer::Record => f.write_str("record"),
                    EmitterPayloadContainer::Array => f.write_str("array"),
                    EmitterPayloadContainer::Table => f.write_str("batch table"),
                    EmitterPayloadContainer::Root => f.write_str("batch root"),
                    EmitterPayloadContainer::SyslogFrame => {
                        f.write_str("RFC 5424 frame with MSG array")
                    }
                    EmitterPayloadContainer::ProtobufMessage(message) => {
                        write!(f, "message {message}")
                    }
                    EmitterPayloadContainer::TransformedValue => f.write_str("transformed value"),
                    EmitterPayloadContainer::Undeclared => f.write_str("container undeclared"),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;

    use super::*;
    use crate::{CodecJaqTransformations, CodecProtobufConfig};

    #[test]
    fn payload_presentation_distinguishes_records_and_every_format_container() {
        let schema = "wire"
            .parse()
            .assured("the fixture wire schema name is valid");
        for (format, expected) in [
            (
                CodecWireFormat::Json {
                    wire_schema: schema,
                },
                "JSON array",
            ),
            (
                CodecWireFormat::Cbor {
                    wire_schema: "wire".parse().assured("valid name"),
                },
                "CBOR array",
            ),
            (
                CodecWireFormat::Avro {
                    wire_schema: "wire".parse().assured("valid name"),
                },
                "AVRO array",
            ),
            (
                CodecWireFormat::Syslog,
                "SYSLOG RFC 5424 frame with MSG array",
            ),
        ] {
            assert_eq!(
                EmitterPayloadPresentation::codec(&format, true).to_string(),
                expected
            );
            assert!(matches!(
                EmitterPayloadPresentation::codec(&format, false),
                EmitterPayloadPresentation::Codec {
                    container: EmitterPayloadContainer::Record,
                    ..
                }
            ));
        }
        for (format, expected) in [
            (CodecJaqFormat::Json, "JSON array"),
            (CodecJaqFormat::Cbor, "CBOR array"),
            (CodecJaqFormat::Yaml, "YAML array"),
            (CodecJaqFormat::Toml, "TOML batch table"),
            (CodecJaqFormat::Xml, "XML batch root"),
        ] {
            let mut format = CodecWireFormat::JaqNative {
                format,
                transformations: CodecJaqTransformations {
                    on_ingestion: None,
                    on_emitting: Some(".".to_owned()),
                    on_emitting_batch: None,
                },
            };
            assert_eq!(
                EmitterPayloadPresentation::codec(&format, true).to_string(),
                expected
            );
            let CodecWireFormat::JaqNative {
                transformations, ..
            } = &mut format
            else {
                unreachable!("the fixture constructed a JAQ codec");
            };
            transformations.on_emitting_batch = Some("{private_value: .}".to_owned());
            let presented = EmitterPayloadPresentation::codec(&format, true).to_string();
            assert!(presented.ends_with("transformed value"));
            assert!(!presented.contains("private_value"));
        }
    }

    #[test]
    fn payload_presentation_names_the_protobuf_batch_message_without_its_program() {
        let mut config = CodecProtobufConfig {
            resource: "bundle"
                .parse()
                .assured("the fixture resource name is valid"),
            resource_version: 1,
            config: Vec::new(),
            message: "event.Record".to_owned(),
            batch_message: Some("event.Batch".to_owned()),
            transformations: CodecJaqTransformations {
                on_ingestion: None,
                on_emitting: Some(".".to_owned()),
                on_emitting_batch: None,
            },
        };
        assert_eq!(
            EmitterPayloadPresentation::codec(&CodecWireFormat::Protobuf(config.clone()), false)
                .to_string(),
            "PROTOBUF message event.Record"
        );
        assert_eq!(
            EmitterPayloadPresentation::codec(&CodecWireFormat::Protobuf(config.clone()), true)
                .to_string(),
            "PROTOBUF message event.Batch"
        );
        config.transformations.on_emitting_batch = Some("{private_value: .}".to_owned());
        assert_eq!(
            EmitterPayloadPresentation::codec(&CodecWireFormat::Protobuf(config.clone()), true)
                .to_string(),
            "PROTOBUF transformed value"
        );
        config.batch_message = None;
        assert_eq!(
            EmitterPayloadPresentation::codec(&CodecWireFormat::Protobuf(config), true).to_string(),
            "PROTOBUF container undeclared"
        );
    }
}
