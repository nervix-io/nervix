//! Codecs: the wire format a payload is read and written in, and the schema it becomes.

use nervix_models::{
    CodecEncoding, CodecEncodingRule, CodecJaqFormat, CodecJaqTransformations, CodecProtobufConfig,
    CodecWireFormat, CreateCodec, RequestedResourceVersion,
};

use crate::Arbitrary;

/// The most encoding rules a generated codec declares.
const ENCODING_RULES: usize = 3;

impl Arbitrary<'_> {
    /// A codec of any wire format, with the transformations and encoding rules that format allows.
    ///
    /// A `SYSLOG` codec's record shape is fixed, so it declares no encoding rules; a codec whose
    /// format runs JAQ programs declares at least one direction, and a batch program only beside
    /// the emitting program it combines.
    pub fn create_codec(&mut self) -> CreateCodec<RequestedResourceVersion> {
        let name = self.name();
        let wire_format = match self.entropy.byte() % 6 {
            0 => CodecWireFormat::Json {
                wire_schema: self.name(),
            },
            1 => CodecWireFormat::Cbor {
                wire_schema: self.name(),
            },
            2 => CodecWireFormat::Avro {
                wire_schema: self.name(),
            },
            3 => CodecWireFormat::Syslog,
            4 => CodecWireFormat::JaqNative {
                format: self.entropy.pick([
                    CodecJaqFormat::Json,
                    CodecJaqFormat::Yaml,
                    CodecJaqFormat::Toml,
                    CodecJaqFormat::Xml,
                    CodecJaqFormat::Cbor,
                ]),
                transformations: self.jaq_transformations(),
            },
            _ => CodecWireFormat::Protobuf(CodecProtobufConfig {
                resource: self.name(),
                resource_version: self.requested_version(),
                config: self.config_entries(),
                message: self.string(),
                batch_message: if self.entropy.flag() {
                    Some(self.string())
                } else {
                    None
                },
                transformations: self.jaq_transformations(),
            }),
        };
        let schema = self.name();
        let encoding_rules = if wire_format == CodecWireFormat::Syslog {
            Vec::new()
        } else {
            let fields = self.distinct_names(0, ENCODING_RULES);
            let mut rules = Vec::with_capacity(fields.len());
            for field in fields {
                rules.push(CodecEncodingRule {
                    field,
                    encoding: CodecEncoding::Rfc3339,
                });
            }
            rules
        };
        CreateCodec {
            name,
            wire_format,
            schema,
            encoding_rules,
        }
    }

    /// JAQ programs for at least one direction, with a batch program only beside an emitting one.
    fn jaq_transformations(&mut self) -> CodecJaqTransformations {
        let (on_ingestion, on_emitting) = match self.entropy.byte() % 3 {
            0 => (Some(self.string()), None),
            1 => (None, Some(self.string())),
            _ => (Some(self.string()), Some(self.string())),
        };
        let on_emitting_batch = if on_emitting.is_some() && self.entropy.flag() {
            Some(self.string())
        } else {
            None
        };
        CodecJaqTransformations {
            on_ingestion,
            on_emitting,
            on_emitting_batch,
        }
    }
}
