use chumsky::prelude::*;
use meticulous::OptionExt as _;
use nervix_models::{
    CodecEncoding, CodecEncodingRule, CodecJaqFormat, CodecJaqTransformations, CodecProtobufConfig,
    CodecWireFormat, CreateCodec, CreateStatement, RequestedResourceVersion, SchemaName,
};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        LexedInput, ParseError, ParseFromSourceError, codec_name, config_entries_block, field_ref,
        if_not_exists_clause, into_parse_error, kw, kw_phrase2, lex_input, resource_ref,
        resource_version_clause, schema_ref, string_lit, suggest_from, tok, wire_avro_schema_ref,
        wire_cbor_schema_ref, wire_json_schema_ref,
    },
};

/// `ON EMITTING` together with the `ON EMITTING BATCH` program that may follow it.
struct EmittingTransformations {
    on_emitting: String,
    on_emitting_batch: Option<String>,
}

impl EmittingTransformations {
    fn into_transformations(
        on_ingestion: Option<String>,
        emitting: Option<Self>,
    ) -> CodecJaqTransformations {
        let Some(emitting) = emitting else {
            return CodecJaqTransformations {
                on_ingestion,
                on_emitting: None,
                on_emitting_batch: None,
            };
        };
        CodecJaqTransformations {
            on_ingestion,
            on_emitting: Some(emitting.on_emitting),
            on_emitting_batch: emitting.on_emitting_batch,
        }
    }
}

/// The part of `CREATE CODEC` that follows `FROM`: the wire format the codec reads together with
/// the wire schema that format needs, the Nervix schema it decodes into, and the per-field
/// encoding rules.
struct CodecBody {
    wire_format: CodecWireFormat<RequestedResourceVersion>,
    schema: SchemaName,
    encoding_rules: Vec<CodecEncodingRule>,
}

pub fn create_codec_parser<'src>() -> impl Parser<
    'src,
    &'src [Token],
    CreateStatement<CreateCodec<RequestedResourceVersion>>,
    extra::Err<ParseError<'src>>,
> + Clone {
    // `ON EMITTING BATCH` builds its input from the `ON EMITTING` outputs, so it is only
    // reachable after that transformation.
    let emitting_batch_transformation = kw(Identifier::On)
        .ignore_then(kw(Identifier::Emitting))
        .ignore_then(kw(Identifier::Batch))
        .ignore_then(string_lit())
        .boxed();
    let emitting_transformations = kw(Identifier::On)
        .ignore_then(kw(Identifier::Emitting))
        .ignore_then(string_lit())
        .then(emitting_batch_transformation.or_not())
        .map(|(on_emitting, on_emitting_batch)| EmittingTransformations {
            on_emitting,
            on_emitting_batch,
        })
        .boxed();
    let ingestion_transformations = kw(Identifier::On)
        .ignore_then(kw(Identifier::Ingestion))
        .ignore_then(string_lit())
        .then(emitting_transformations.clone().or_not())
        .map(|(on_ingestion, emitting)| {
            EmittingTransformations::into_transformations(Some(on_ingestion), emitting)
        });
    let emitting_only_transformations = emitting_transformations
        .map(|emitting| EmittingTransformations::into_transformations(None, Some(emitting)));
    let directed_jaq_transformations =
        choice((ingestion_transformations, emitting_only_transformations)).boxed();
    let jaq_transformations = kw(Identifier::With)
        .ignore_then(kw(Identifier::Jaq))
        .ignore_then(kw(Identifier::Transformations))
        .ignore_then(directed_jaq_transformations)
        .boxed();

    let encoding_rule = field_ref()
        .then_ignore(kw(Identifier::As))
        .then(kw(Identifier::Rfc3339).to(CodecEncoding::Rfc3339))
        .map(|(field, encoding)| CodecEncodingRule { field, encoding })
        .boxed();
    let encoding_rules = kw(Identifier::Encode)
        .ignore_then(
            encoding_rule
                .separated_by(tok(Token::Comma))
                .allow_trailing()
                .at_least(1)
                .collect::<Vec<_>>(),
        )
        .or_not()
        .map(Option::unwrap_or_default)
        .boxed();

    let json_wire = kw(Identifier::Json)
        .ignore_then(kw(Identifier::Schema))
        .ignore_then(wire_json_schema_ref())
        .map(|wire_schema| CodecWireFormat::Json { wire_schema });
    let cbor_wire = kw(Identifier::Cbor)
        .ignore_then(kw(Identifier::Schema))
        .ignore_then(wire_cbor_schema_ref())
        .map(|wire_schema| CodecWireFormat::Cbor { wire_schema });
    let avro_wire = kw(Identifier::Avro)
        .ignore_then(kw(Identifier::Schema))
        .ignore_then(wire_avro_schema_ref())
        .map(|wire_schema| CodecWireFormat::Avro { wire_schema });
    let schemaful_codec = kw(Identifier::Wire)
        .ignore_then(choice((json_wire, cbor_wire, avro_wire)))
        .then_ignore(kw(Identifier::To))
        .then_ignore(kw(Identifier::Schema))
        .then(schema_ref())
        .boxed()
        .then(encoding_rules.clone())
        .map(|((wire_format, schema), encoding_rules)| CodecBody {
            wire_format,
            schema,
            encoding_rules,
        })
        .boxed();
    let jaq_format = choice((
        kw(Identifier::Json).to(CodecJaqFormat::Json),
        kw(Identifier::Yaml).to(CodecJaqFormat::Yaml),
        kw(Identifier::Toml).to(CodecJaqFormat::Toml),
        kw(Identifier::Xml).to(CodecJaqFormat::Xml),
        kw(Identifier::Cbor).to(CodecJaqFormat::Cbor),
    ))
    .boxed();
    let jaq_native_codec = jaq_format
        .then_ignore(kw(Identifier::To))
        .then_ignore(kw(Identifier::Schema))
        .then(schema_ref())
        .then(jaq_transformations.clone())
        .boxed()
        .then(encoding_rules.clone())
        .map(
            |(((format, schema), transformations), encoding_rules)| CodecBody {
                wire_format: CodecWireFormat::JaqNative {
                    format,
                    transformations,
                },
                schema,
                encoding_rules,
            },
        )
        .boxed();
    let protobuf_codec = kw(Identifier::Protobuf)
        .ignore_then(kw(Identifier::Using))
        .ignore_then(kw(Identifier::Resource))
        .ignore_then(resource_ref())
        .then(resource_version_clause())
        .then(config_entries_block())
        .boxed()
        .then_ignore(kw(Identifier::Message))
        .then(string_lit())
        .then(
            kw_phrase2(Identifier::Batch, Identifier::Message)
                .ignore_then(string_lit())
                .or_not(),
        )
        .then_ignore(kw(Identifier::To))
        .then_ignore(kw(Identifier::Schema))
        .then(schema_ref())
        .boxed()
        .then(jaq_transformations)
        .then(encoding_rules)
        .map(
            |(
                (
                    (((((resource, resource_version), config), message), batch_message), schema),
                    transformations,
                ),
                encoding_rules,
            )| {
                CodecBody {
                    wire_format: CodecWireFormat::Protobuf(CodecProtobufConfig {
                        resource,
                        resource_version,
                        config,
                        message,
                        batch_message,
                        transformations,
                    }),
                    schema,
                    encoding_rules,
                }
            },
        )
        .boxed();
    let syslog_codec = kw(Identifier::Syslog)
        .ignore_then(kw(Identifier::To))
        .ignore_then(kw(Identifier::Schema))
        .ignore_then(schema_ref())
        .map(|schema| CodecBody {
            wire_format: CodecWireFormat::Syslog,
            schema,
            encoding_rules: Vec::new(),
        })
        .boxed();

    kw(Identifier::Create)
        .ignore_then(if_not_exists_clause())
        .then_ignore(kw(Identifier::Codec))
        .then(codec_name())
        .then_ignore(kw(Identifier::From))
        .boxed()
        .then(
            choice((
                schemaful_codec,
                protobuf_codec,
                jaq_native_codec,
                syslog_codec,
            ))
            .boxed(),
        )
        .then_ignore(tok(Token::Semicolon).or_not())
        .map(|((if_not_exists, name), body)| {
            CreateStatement::new(
                CreateCodec {
                    name,
                    wire_format: body.wire_format,
                    schema: body.schema,
                    encoding_rules: body.encoding_rules,
                },
                if_not_exists,
            )
        })
        .boxed()
}

pub fn parse_create_codec_tokens(
    tokens: &[Token],
) -> Result<CreateStatement<CreateCodec<RequestedResourceVersion>>, Vec<ParseError<'_>>> {
    let out = create_codec_parser().then_ignore(end()).parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .verified("has_errors returned false above, so this parse produced output"))
    }
}

pub fn parse_create_codec(
    input: &str,
) -> Result<CreateStatement<CreateCodec<RequestedResourceVersion>>, ParseFromSourceError> {
    let LexedInput {
        source,
        spanned_tokens,
        tokens,
    } = lex_input(input)?;
    parse_create_codec_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_create_codec(input: &str, cursor: usize) -> Vec<String> {
    suggest_from!(input, cursor, create_codec_parser())
}

#[cfg(test)]
mod tests {
    use nervix_models::{ClientConfigEntry, WireSchemaName};
    use rstest::rstest;

    use super::*;
    use crate::lexer::lex;

    fn to_tokens(input: &str) -> Vec<Token> {
        lex(input)
            .expect("lexer should succeed")
            .into_iter()
            .map(|t| t.token)
            .collect()
    }

    #[rstest]
    #[case::xml_with_both_transformations(
        "CREATE CODEC notification_codec FROM XML TO SCHEMA notification_schema WITH JAQ \
         TRANSFORMATIONS ON INGESTION \".payload\" ON EMITTING \"{payload: {user_id}}\";",
        CodecJaqFormat::Xml,
        Some(".payload"),
        Some("{payload: {user_id}}")
    )]
    #[case::yaml_with_ingestion_transformation(
        "CREATE CODEC notification_codec FROM YAML TO SCHEMA notification_schema WITH JAQ \
         TRANSFORMATIONS ON INGESTION \".payload\";",
        CodecJaqFormat::Yaml,
        Some(".payload"),
        None
    )]
    #[case::json_with_emitting_transformation(
        "CREATE CODEC notification_codec FROM JSON TO SCHEMA notification_schema WITH JAQ \
         TRANSFORMATIONS ON EMITTING \"{payload: .}\";",
        CodecJaqFormat::Json,
        None,
        Some("{payload: .}")
    )]
    #[case::cbor_with_ingestion_transformation(
        "CREATE CODEC notification_codec FROM CBOR TO SCHEMA notification_schema WITH JAQ \
         TRANSFORMATIONS ON INGESTION \".payload\";",
        CodecJaqFormat::Cbor,
        Some(".payload"),
        None
    )]
    fn parses_jaq_native_codec_transformations(
        #[case] input: &str,
        #[case] format: CodecJaqFormat,
        #[case] on_ingestion: Option<&str>,
        #[case] on_emitting: Option<&str>,
    ) {
        let parsed = parse_create_codec_tokens(&to_tokens(input)).expect("parse should succeed");

        assert_eq!(
            parsed.wire_format,
            CodecWireFormat::JaqNative {
                format,
                transformations: CodecJaqTransformations {
                    on_ingestion: on_ingestion.map(str::to_string),
                    on_emitting: on_emitting.map(str::to_string),
                    on_emitting_batch: None,
                },
            }
        );
    }

    #[test]
    fn parses_create_protobuf_codec_with_resource_config_and_message() {
        let tokens = to_tokens(
            "CREATE CODEC notification_codec FROM PROTOBUF USING RESOURCE proto_bundle VERSION 2 \
             CONFIG {\"file\" = \"notification.proto\", \"include\" = \".\"} MESSAGE \
             \"nervix.test.Notification\" TO SCHEMA notification_schema WITH JAQ TRANSFORMATIONS \
             ON INGESTION \".\" ON EMITTING \".\";",
        );
        let parsed = parse_create_codec_tokens(&tokens).expect("parse should succeed");

        assert_eq!(
            parsed.wire_format,
            CodecWireFormat::Protobuf(CodecProtobufConfig {
                resource: nervix_models::ResourceName::parse("proto_bundle")
                    .expect("valid identifier"),
                resource_version: RequestedResourceVersion::Number(2),
                config: vec![
                    ClientConfigEntry {
                        key: "file".to_string(),
                        value: "notification.proto".to_string(),
                    },
                    ClientConfigEntry {
                        key: "include".to_string(),
                        value: ".".to_string(),
                    },
                ],
                message: "nervix.test.Notification".to_string(),
                batch_message: None,
                transformations: CodecJaqTransformations {
                    on_ingestion: Some(".".to_string()),
                    on_emitting: Some(".".to_string()),
                    on_emitting_batch: None,
                },
            })
        );
        assert_eq!(parsed.schema.as_str(), "notification_schema");
    }

    #[test]
    fn parses_create_protobuf_codec_bound_to_the_latest_resource_version() {
        let parsed = parse_create_codec(
            "CREATE CODEC notification_codec FROM PROTOBUF USING RESOURCE proto_bundle VERSION \
             LATEST CONFIG {'file' = 'notification.proto'} MESSAGE 'nervix.test.Notification' TO \
             SCHEMA notification_schema WITH JAQ TRANSFORMATIONS ON INGESTION '.';",
        )
        .expect("parse should succeed");

        let CodecWireFormat::Protobuf(config) = &parsed.wire_format else {
            panic!("expected a protobuf codec");
        };
        assert_eq!(config.resource_version, RequestedResourceVersion::Latest);
    }

    fn wire_schema_name(name: &str) -> WireSchemaName {
        WireSchemaName::parse(name).expect("valid identifier")
    }

    #[rstest]
    #[case::json(
        "CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA \
         notification_schema;",
        CodecWireFormat::Json { wire_schema: wire_schema_name("notification_wire") },
        "notification_codec",
        "notification_schema"
    )]
    #[case::avro(
        "CREATE CODEC notification_codec FROM WIRE AVRO SCHEMA notification_wire TO SCHEMA \
         notification_schema;",
        CodecWireFormat::Avro { wire_schema: wire_schema_name("notification_wire") },
        "notification_codec",
        "notification_schema"
    )]
    #[case::cbor(
        "CREATE CODEC notification_codec FROM WIRE CBOR SCHEMA notification_wire TO SCHEMA \
         notification_schema;",
        CodecWireFormat::Cbor { wire_schema: wire_schema_name("notification_wire") },
        "notification_codec",
        "notification_schema"
    )]
    #[case::syslog(
        "CREATE CODEC events FROM SYSLOG TO SCHEMA syslog_event;",
        CodecWireFormat::Syslog,
        "events",
        "syslog_event"
    )]
    fn parses_schemaful_codec_formats(
        #[case] input: &str,
        #[case] wire_format: CodecWireFormat<RequestedResourceVersion>,
        #[case] name: &str,
        #[case] schema: &str,
    ) {
        let parsed = parse_create_codec(input).expect("parse should succeed");

        assert_eq!(parsed.name.as_str(), name);
        assert_eq!(parsed.wire_format, wire_format);
        assert!(parsed.encoding_rules.is_empty());
        assert_eq!(parsed.schema.as_str(), schema);
    }

    #[rstest]
    #[case::implicit_jaq_transformation(
        "CREATE CODEC notification_codec FROM YAML TO SCHEMA notification_schema WITH JAQ \
         TRANSFORMATION \".payload\";"
    )]
    #[case::syslog_jaq_transformation(
        "CREATE CODEC events FROM SYSLOG TO SCHEMA syslog_event WITH JAQ TRANSFORMATIONS ON \
         INGESTION '.';"
    )]
    #[case::syslog_encoding_rule(
        "CREATE CODEC events FROM SYSLOG TO SCHEMA syslog_event ENCODE timestamp AS RFC3339;"
    )]
    #[case::incomplete_encoding_rule(
        "CREATE CODEC orders_codec FROM WIRE JSON SCHEMA orders_wire TO SCHEMA orders ENCODE \
         created_at RFC3339;"
    )]
    #[case::jaq_native_without_transformation(
        "CREATE CODEC notification_codec FROM XML TO SCHEMA notification_schema;"
    )]
    #[case::protobuf_without_jaq_transformation(
        "CREATE CODEC notification_codec FROM PROTOBUF USING RESOURCE proto_bundle VERSION 1 \
         CONFIG {\"file\" = \"notification.proto\"} MESSAGE \"nervix.test.Notification\" TO \
         SCHEMA notification_schema;"
    )]
    #[case::protobuf_without_config_clause(
        "CREATE CODEC notification_codec FROM PROTOBUF USING RESOURCE proto_bundle VERSION 1 \
         MESSAGE \"nervix.test.Notification\" TO SCHEMA notification_schema WITH JAQ \
         TRANSFORMATIONS ON INGESTION \".\";"
    )]
    #[case::protobuf_without_resource_version(
        "CREATE CODEC notification_codec FROM PROTOBUF USING RESOURCE proto_bundle CONFIG \
         {\"file\" = \"notification.proto\"} MESSAGE \"nervix.test.Notification\" TO SCHEMA \
         notification_schema WITH JAQ TRANSFORMATIONS ON INGESTION \".\";"
    )]
    #[case::protobuf_with_a_named_version(
        "CREATE CODEC notification_codec FROM PROTOBUF USING RESOURCE proto_bundle VERSION newest \
         CONFIG {\"file\" = \"notification.proto\"} MESSAGE \"nervix.test.Notification\" TO \
         SCHEMA notification_schema WITH JAQ TRANSFORMATIONS ON INGESTION \".\";"
    )]
    #[case::without_explicit_wire_format(
        "CREATE CODEC notification_codec FROM WIRE SCHEMA notification_wire TO SCHEMA \
         notification_schema;"
    )]
    #[case::jaq_transformations_without_direction(
        "CREATE CODEC notification_codec FROM XML TO SCHEMA notification_schema WITH JAQ \
         TRANSFORMATIONS \".payload\";"
    )]
    #[case::schemaful_with_jaq_transformation(
        "CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA \
         notification_schema WITH JAQ TRANSFORMATIONS ON INGESTION \".payload\";"
    )]
    #[case::batch_transformation_without_emitting(
        "CREATE CODEC notification_codec FROM JSON TO SCHEMA notification_schema WITH JAQ \
         TRANSFORMATIONS ON EMITTING BATCH \".\";"
    )]
    #[case::batch_transformation_after_ingestion_only(
        "CREATE CODEC notification_codec FROM JSON TO SCHEMA notification_schema WITH JAQ \
         TRANSFORMATIONS ON INGESTION \".\" ON EMITTING BATCH \".\";"
    )]
    #[case::batch_transformation_before_emitting(
        "CREATE CODEC notification_codec FROM JSON TO SCHEMA notification_schema WITH JAQ \
         TRANSFORMATIONS ON EMITTING BATCH \".\" ON EMITTING \".\";"
    )]
    #[case::batch_message_before_message(
        "CREATE CODEC notification_codec FROM PROTOBUF USING RESOURCE proto_bundle VERSION 1 \
         CONFIG {\"file\" = \"notification.proto\"} BATCH MESSAGE \"nervix.test.Batch\" MESSAGE \
         \"nervix.test.Notification\" TO SCHEMA notification_schema WITH JAQ TRANSFORMATIONS ON \
         EMITTING \".\";"
    )]
    #[case::batch_message_on_a_jaq_native_codec(
        "CREATE CODEC notification_codec FROM JSON BATCH MESSAGE \"nervix.test.Batch\" TO SCHEMA \
         notification_schema WITH JAQ TRANSFORMATIONS ON EMITTING \".\";"
    )]
    fn rejects_invalid_codec_forms(#[case] input: &str) {
        assert!(parse_create_codec(input).is_err());
    }

    #[test]
    fn parses_create_codec_with_rfc3339_datetime_encoding() {
        let tokens = to_tokens(
            "CREATE CODEC orders_codec FROM WIRE JSON SCHEMA orders_wire TO SCHEMA orders ENCODE \
             created_at AS RFC3339;",
        );
        let parsed = parse_create_codec_tokens(&tokens).expect("parse should succeed");

        assert_eq!(
            parsed.encoding_rules,
            vec![CodecEncodingRule {
                field: nervix_models::FieldName::parse("created_at").expect("valid field name"),
                encoding: CodecEncoding::Rfc3339,
            }]
        );
    }

    #[rstest]
    #[case::encode_after_target_schema(
        "CREATE CODEC orders_codec FROM WIRE JSON SCHEMA orders_wire TO SCHEMA orders EN",
        &["ENCODE"],
        &[]
    )]
    #[case::from_after_codec_name(
        "CREATE CODEC notification_codec ",
        &["FROM"],
        &[]
    )]
    #[case::schemaful_formats_after_from_wire(
        "CREATE CODEC notification_codec FROM WIRE ",
        &["JSON", "AVRO", "CBOR"],
        &[]
    )]
    #[case::wire_and_jaq_native_formats_after_from(
        "CREATE CODEC notification_codec FROM ",
        &["WIRE", "JSON", "YAML", "TOML", "XML", "CBOR", "PROTOBUF", "SYSLOG"],
        &[]
    )]
    #[case::syslog_stays_on_its_codec_branch(
        "CREATE CODEC events FROM SYSLOG ",
        &["TO"],
        &["USING", "WITH", "ENCODE"]
    )]
    #[case::using_after_from_protobuf(
        "CREATE CODEC notification_codec FROM PROTOBUF ",
        &["USING"],
        &["WIRE"]
    )]
    #[case::only_version_after_protobuf_resource(
        "CREATE CODEC notification_codec FROM PROTOBUF USING RESOURCE proto_bundle ",
        &["VERSION"],
        &["CONFIG", "MESSAGE"]
    )]
    #[case::latest_and_completed_version_after_protobuf_version(
        "CREATE CODEC notification_codec FROM PROTOBUF USING RESOURCE proto_bundle VERSION ",
        &["LATEST", "completed_resource_version"],
        &["CONFIG", "integer_literal"]
    )]
    #[case::config_after_protobuf_latest_resource_version(
        "CREATE CODEC notification_codec FROM PROTOBUF USING RESOURCE proto_bundle VERSION LATEST ",
        &["CONFIG"],
        &["MESSAGE", "LATEST"]
    )]
    #[case::config_after_protobuf_resource_version(
        "CREATE CODEC notification_codec FROM PROTOBUF USING RESOURCE proto_bundle VERSION 1 ",
        &["CONFIG"],
        &["MESSAGE"]
    )]
    #[case::on_after_with_jaq_transformations(
        "CREATE CODEC notification_codec FROM XML TO SCHEMA notification_schema WITH JAQ \
         TRANSFORMATIONS ",
        &["ON"],
        &["TO"]
    )]
    #[case::both_directions_after_with_jaq_transformations_on(
        "CREATE CODEC notification_codec FROM XML TO SCHEMA notification_schema WITH JAQ \
         TRANSFORMATIONS ON ",
        &["INGESTION", "EMITTING"],
        &["TRANSFORMATIONS"]
    )]
    #[case::only_emitting_as_second_jaq_transformation_direction(
        "CREATE CODEC notification_codec FROM XML TO SCHEMA notification_schema WITH JAQ \
         TRANSFORMATIONS ON INGESTION \".\" ON ",
        &["EMITTING"],
        &["INGESTION"]
    )]
    #[case::emitting_batch_after_emitting_transformation(
        "CREATE CODEC notification_codec FROM JSON TO SCHEMA notification_schema WITH JAQ \
         TRANSFORMATIONS ON EMITTING \".\" ON EMITTING ",
        &["BATCH"],
        &["INGESTION"]
    )]
    #[case::emitting_after_ingestion_and_emitting_transformations(
        "CREATE CODEC notification_codec FROM JSON TO SCHEMA notification_schema WITH JAQ \
         TRANSFORMATIONS ON INGESTION \".\" ON EMITTING \".\" ON ",
        &["EMITTING"],
        &["INGESTION"]
    )]
    #[case::batch_message_after_protobuf_message(
        "CREATE CODEC notification_codec FROM PROTOBUF USING RESOURCE proto_bundle VERSION 1 \
         CONFIG {\"file\" = \"notification.proto\"} MESSAGE \"nervix.test.Notification\" ",
        &["BATCH MESSAGE", "TO"],
        &["BATCH", "WITH"]
    )]
    #[case::only_explicit_transformations_after_with_jaq(
        "CREATE CODEC notification_codec FROM XML TO SCHEMA notification_schema WITH JAQ ",
        &["TRANSFORMATIONS"],
        &["TRANSFORMATION"]
    )]
    fn completes_codec_context_without_branch_leakage(
        #[case] input: &str,
        #[case] expected: &[&str],
        #[case] rejected: &[&str],
    ) {
        let suggestions = suggest_create_codec(input, input.len());

        for expected in expected {
            assert!(suggestions.iter().any(|suggestion| suggestion == expected));
        }
        for rejected in rejected {
            assert!(!suggestions.iter().any(|suggestion| suggestion == rejected));
        }
    }

    #[test]
    fn parses_an_emitting_batch_transformation_after_the_emitting_transformation() {
        for (input, on_ingestion) in [
            (
                "CREATE CODEC notification_codec FROM JSON TO SCHEMA notification_schema WITH JAQ \
                 TRANSFORMATIONS ON EMITTING \"{id: .user_id}\" ON EMITTING BATCH \"{records: \
                 .}\";",
                None,
            ),
            (
                "CREATE CODEC notification_codec FROM JSON TO SCHEMA notification_schema WITH JAQ \
                 TRANSFORMATIONS ON INGESTION \".\" ON EMITTING \"{id: .user_id}\" ON EMITTING \
                 BATCH \"{records: .}\";",
                Some(".".to_string()),
            ),
        ] {
            let parsed = parse_create_codec(input).expect("the codec parses");

            assert_eq!(
                parsed.wire_format,
                CodecWireFormat::JaqNative {
                    format: CodecJaqFormat::Json,
                    transformations: CodecJaqTransformations {
                        on_ingestion,
                        on_emitting: Some("{id: .user_id}".to_string()),
                        on_emitting_batch: Some("{records: .}".to_string()),
                    },
                }
            );
        }
    }

    #[test]
    fn parses_a_protobuf_batch_message_after_the_message() {
        let parsed = parse_create_codec(
            "CREATE CODEC notification_codec FROM PROTOBUF USING RESOURCE proto_bundle VERSION 1 \
             CONFIG {\"file\" = \"notification.proto\"} MESSAGE \"nervix.test.Notification\" \
             BATCH MESSAGE \"nervix.test.NotificationBatch\" TO SCHEMA notification_schema WITH \
             JAQ TRANSFORMATIONS ON EMITTING \".\";",
        )
        .expect("the protobuf codec parses");

        let CodecWireFormat::Protobuf(config) = &parsed.wire_format else {
            panic!("expected a protobuf codec, got {:?}", parsed.wire_format);
        };
        assert_eq!(config.message, "nervix.test.Notification");
        assert_eq!(
            config.batch_message.as_deref(),
            Some("nervix.test.NotificationBatch")
        );
    }
}
