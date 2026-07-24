use chumsky::prelude::*;
use nervix_models::{
    ClientConfigEntry, CodecEncoding, CodecEncodingRule, CodecJaqFormat, CodecJaqTransformations,
    CodecProtobufConfig, CodecWireFormat, CreateCodec, CreateStatement,
};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, ParseFromSourceError, codec_name, current_word_prefix, field_ref,
        if_not_exists_clause, into_parse_error, kw, lex_input, resource_ref, schema_ref,
        string_lit, suggestions_from_errors, tok, u64_value, wire_schema_ref,
    },
};

fn scalar_config_value<'src>()
-> impl Parser<'src, &'src [Token], String, extra::Err<ParseError<'src>>> + Clone {
    choice((
        string_lit(),
        select! { Token::NumberLiteral(v) => v },
        crate::parser_support::word_raw(),
    ))
}

fn protobuf_config_entry<'src>()
-> impl Parser<'src, &'src [Token], ClientConfigEntry, extra::Err<ParseError<'src>>> + Clone {
    string_lit()
        .labelled("config_key")
        .then_ignore(tok(Token::Eq))
        .then(scalar_config_value().labelled("config_value"))
        .map(|(key, value)| ClientConfigEntry { key, value })
}

fn protobuf_config<'src>()
-> impl Parser<'src, &'src [Token], Vec<ClientConfigEntry>, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Config).ignore_then(
        protobuf_config_entry()
            .separated_by(tok(Token::Comma))
            .allow_trailing()
            .collect::<Vec<_>>()
            .delimited_by(tok(Token::LBrace), tok(Token::RBrace)),
    )
}

pub fn create_codec_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateCodec>, extra::Err<ParseError<'src>>> + Clone
{
    let ingestion_transformations = kw(Identifier::On)
        .ignore_then(kw(Identifier::Ingestion))
        .ignore_then(string_lit())
        .then(
            kw(Identifier::On)
                .ignore_then(kw(Identifier::Emitting))
                .ignore_then(string_lit())
                .or_not(),
        )
        .map(|(on_ingestion, on_emitting)| CodecJaqTransformations {
            on_ingestion: Some(on_ingestion),
            on_emitting,
        });
    let emitting_transformations = kw(Identifier::On)
        .ignore_then(kw(Identifier::Emitting))
        .ignore_then(string_lit())
        .map(|on_emitting| CodecJaqTransformations {
            on_ingestion: None,
            on_emitting: Some(on_emitting),
        });
    let directed_jaq_transformations =
        choice((ingestion_transformations, emitting_transformations)).boxed();
    let jaq_transformations = kw(Identifier::With)
        .ignore_then(kw(Identifier::Jaq))
        .ignore_then(choice((
            kw(Identifier::Transformation)
                .ignore_then(string_lit())
                .map(|on_ingestion| CodecJaqTransformations {
                    on_ingestion: Some(on_ingestion),
                    on_emitting: None,
                }),
            kw(Identifier::Transformations).ignore_then(directed_jaq_transformations),
        )))
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
        .ignore_then(wire_schema_ref())
        .map(|wire_schema| (CodecWireFormat::Json, Some(wire_schema)));
    let cbor_wire = kw(Identifier::Cbor)
        .ignore_then(kw(Identifier::Schema))
        .ignore_then(wire_schema_ref())
        .map(|wire_schema| (CodecWireFormat::Cbor, Some(wire_schema)));
    let avro_wire = kw(Identifier::Avro)
        .ignore_then(kw(Identifier::Schema))
        .ignore_then(wire_schema_ref())
        .map(|wire_schema| (CodecWireFormat::Avro, Some(wire_schema)));
    let schemaful_codec = kw(Identifier::Wire)
        .ignore_then(choice((json_wire, cbor_wire, avro_wire)))
        .then_ignore(kw(Identifier::To))
        .then_ignore(kw(Identifier::Schema))
        .then(schema_ref())
        .boxed()
        .then(encoding_rules.clone())
        .map(|(((wire_format, wire_schema), schema), encoding_rules)| {
            (wire_format, wire_schema, schema, encoding_rules)
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
        .map(|(((format, schema), transformations), encoding_rules)| {
            (
                CodecWireFormat::JaqNative {
                    format,
                    transformations,
                },
                None,
                schema,
                encoding_rules,
            )
        })
        .boxed();
    let protobuf_codec = kw(Identifier::Protobuf)
        .ignore_then(kw(Identifier::Using))
        .ignore_then(kw(Identifier::Resource))
        .ignore_then(resource_ref())
        .then(kw(Identifier::Version).ignore_then(u64_value()).or_not())
        .then(protobuf_config())
        .boxed()
        .then_ignore(kw(Identifier::Message))
        .then(string_lit())
        .then_ignore(kw(Identifier::To))
        .then_ignore(kw(Identifier::Schema))
        .then(schema_ref())
        .boxed()
        .then(jaq_transformations)
        .then(encoding_rules)
        .map(
            |(
                (((((resource, resource_version), config), message), schema), transformations),
                encoding_rules,
            )| {
                (
                    CodecWireFormat::Protobuf(CodecProtobufConfig {
                        resource,
                        resource_version,
                        config,
                        message,
                        transformations,
                    }),
                    None,
                    schema,
                    encoding_rules,
                )
            },
        )
        .boxed();

    kw(Identifier::Create)
        .ignore_then(if_not_exists_clause())
        .then_ignore(kw(Identifier::Codec))
        .then(codec_name())
        .then_ignore(kw(Identifier::From))
        .boxed()
        .then(choice((schemaful_codec, protobuf_codec, jaq_native_codec)).boxed())
        .then_ignore(tok(Token::Semicolon).or_not())
        .map(
            |((if_not_exists, name), (wire_format, wire_schema, schema, encoding_rules))| {
                CreateStatement::new(
                    CreateCodec {
                        name,
                        wire_format,
                        wire_schema,
                        schema,
                        encoding_rules,
                    },
                    if_not_exists,
                )
            },
        )
        .boxed()
}

pub fn parse_create_codec_tokens(
    tokens: &[Token],
) -> Result<CreateStatement<CreateCodec>, Vec<ParseError<'_>>> {
    let out = create_codec_parser().then_ignore(end()).parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .expect("successful parse must have output"))
    }
}

pub fn parse_create_codec(
    input: &str,
) -> Result<CreateStatement<CreateCodec>, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_create_codec_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_create_codec(input: &str, cursor: usize) -> Vec<String> {
    let safe_cursor = cursor.min(input.len());
    let prefix_src = &input[..safe_cursor];
    let prefix = current_word_prefix(prefix_src);

    let (_, _, tokens) = match lex_input(prefix_src) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    let out = create_codec_parser()
        .then_ignore(end())
        .parse(tokens.as_slice());
    if !out.has_errors() {
        return Vec::new();
    }

    suggestions_from_errors(out.into_errors(), &prefix)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lexer::lex;

    fn to_tokens(input: &str) -> Vec<Token> {
        lex(input)
            .expect("lexer should succeed")
            .into_iter()
            .map(|t| t.token)
            .collect()
    }

    #[test]
    fn parses_create_codec() {
        let tokens = to_tokens(
            "CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA \
             notification_schema;",
        );
        let parsed = parse_create_codec_tokens(&tokens).expect("parse should succeed");

        assert_eq!(parsed.name.as_str(), "notification_codec");
        assert_eq!(parsed.wire_format, CodecWireFormat::Json);
        assert_eq!(
            parsed
                .wire_schema
                .as_ref()
                .map(|wire_schema| wire_schema.as_str()),
            Some("notification_wire")
        );
        assert_eq!(parsed.schema.as_str(), "notification_schema");
    }

    #[test]
    fn parses_create_jaq_native_codec_with_transformations() {
        let tokens = to_tokens(
            "CREATE CODEC notification_codec FROM XML TO SCHEMA notification_schema WITH JAQ \
             TRANSFORMATIONS ON INGESTION \".payload\" ON EMITTING \"{payload: {user_id}}\";",
        );
        let parsed = parse_create_codec_tokens(&tokens).expect("parse should succeed");

        assert_eq!(
            parsed.wire_format,
            CodecWireFormat::JaqNative {
                format: CodecJaqFormat::Xml,
                transformations: CodecJaqTransformations {
                    on_ingestion: Some(".payload".to_string()),
                    on_emitting: Some("{payload: {user_id}}".to_string()),
                },
            }
        );
        assert_eq!(parsed.wire_schema, None);
    }

    #[test]
    fn parses_create_jaq_native_codec_with_shorthand_ingestion_transformation() {
        let tokens = to_tokens(
            "CREATE CODEC notification_codec FROM YAML TO SCHEMA notification_schema WITH JAQ \
             TRANSFORMATION \".payload\";",
        );
        let parsed = parse_create_codec_tokens(&tokens).expect("parse should succeed");

        assert_eq!(
            parsed.wire_format,
            CodecWireFormat::JaqNative {
                format: CodecJaqFormat::Yaml,
                transformations: CodecJaqTransformations {
                    on_ingestion: Some(".payload".to_string()),
                    on_emitting: None,
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
                resource: nervix_models::Identifier::parse("proto_bundle")
                    .expect("valid identifier"),
                resource_version: Some(2),
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
                transformations: CodecJaqTransformations {
                    on_ingestion: Some(".".to_string()),
                    on_emitting: Some(".".to_string()),
                },
            })
        );
        assert_eq!(parsed.wire_schema, None);
        assert_eq!(parsed.schema.as_str(), "notification_schema");
    }

    #[test]
    fn parses_create_avro_codec() {
        let tokens = to_tokens(
            "CREATE CODEC notification_codec FROM WIRE AVRO SCHEMA notification_wire TO SCHEMA \
             notification_schema;",
        );
        let parsed = parse_create_codec_tokens(&tokens).expect("parse should succeed");

        assert_eq!(parsed.wire_format, CodecWireFormat::Avro);
    }

    #[test]
    fn parses_create_schemaful_cbor_codec() {
        let tokens = to_tokens(
            "CREATE CODEC notification_codec FROM WIRE CBOR SCHEMA notification_wire TO SCHEMA \
             notification_schema;",
        );
        let parsed = parse_create_codec_tokens(&tokens).expect("parse should succeed");

        assert_eq!(parsed.wire_format, CodecWireFormat::Cbor);
        assert_eq!(
            parsed
                .wire_schema
                .as_ref()
                .map(|wire_schema| wire_schema.as_str()),
            Some("notification_wire")
        );
        assert_eq!(parsed.schema.as_str(), "notification_schema");
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
                field: nervix_models::Identifier::parse("created_at").expect("valid identifier"),
                encoding: CodecEncoding::Rfc3339,
            }]
        );
    }

    #[test]
    fn rejects_create_codec_with_incomplete_encoding_rule() {
        let input = "CREATE CODEC orders_codec FROM WIRE JSON SCHEMA orders_wire TO SCHEMA orders \
                     ENCODE created_at RFC3339;";

        assert!(parse_create_codec(input).is_err());
    }

    #[test]
    fn suggests_encode_after_codec_target_schema() {
        let input =
            "CREATE CODEC orders_codec FROM WIRE JSON SCHEMA orders_wire TO SCHEMA orders EN";
        let suggestions = suggest_create_codec(input, input.len());
        assert!(suggestions.contains(&"ENCODE".to_string()));
    }

    #[test]
    fn parses_create_cbor_jaq_native_codec() {
        let tokens = to_tokens(
            "CREATE CODEC notification_codec FROM CBOR TO SCHEMA notification_schema WITH JAQ \
             TRANSFORMATION \".payload\";",
        );
        let parsed = parse_create_codec_tokens(&tokens).expect("parse should succeed");

        assert_eq!(
            parsed.wire_format,
            CodecWireFormat::JaqNative {
                format: CodecJaqFormat::Cbor,
                transformations: CodecJaqTransformations {
                    on_ingestion: Some(".payload".to_string()),
                    on_emitting: None,
                },
            }
        );
        assert_eq!(parsed.wire_schema, None);
    }

    #[test]
    fn rejects_jaq_native_codec_without_jaq_transformation() {
        let input = "CREATE CODEC notification_codec FROM XML TO SCHEMA notification_schema;";

        assert!(parse_create_codec(input).is_err());
    }

    #[test]
    fn rejects_protobuf_codec_without_jaq_transformation() {
        let input = "CREATE CODEC notification_codec FROM PROTOBUF USING RESOURCE proto_bundle \
                     CONFIG {\"file\" = \"notification.proto\"} MESSAGE \
                     \"nervix.test.Notification\" TO SCHEMA notification_schema;";

        assert!(parse_create_codec(input).is_err());
    }

    #[test]
    fn rejects_protobuf_codec_without_config_clause() {
        let input = "CREATE CODEC notification_codec FROM PROTOBUF USING RESOURCE proto_bundle \
                     MESSAGE \"nervix.test.Notification\" TO SCHEMA notification_schema WITH JAQ \
                     TRANSFORMATION \".\";";

        assert!(parse_create_codec(input).is_err());
    }

    #[test]
    fn rejects_create_codec_without_explicit_wire_format() {
        let input = "CREATE CODEC notification_codec FROM WIRE SCHEMA notification_wire TO SCHEMA \
                     notification_schema;";

        assert!(parse_create_codec(input).is_err());
    }

    #[test]
    fn rejects_create_codec_with_jaq_transformations_without_direction() {
        let input = "CREATE CODEC notification_codec FROM XML TO SCHEMA notification_schema WITH \
                     JAQ TRANSFORMATIONS \".payload\";";

        assert!(parse_create_codec(input).is_err());
    }

    #[test]
    fn rejects_schemaful_codec_with_jaq_transformation() {
        let input = "CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO \
                     SCHEMA notification_schema WITH JAQ TRANSFORMATION \".payload\";";

        assert!(parse_create_codec(input).is_err());
    }

    #[test]
    fn suggests_from_after_codec_name() {
        let input = "CREATE CODEC notification_codec ";
        let suggestions = suggest_create_codec(input, input.len());
        assert!(suggestions.contains(&"FROM".to_string()));
    }

    #[test]
    fn suggests_schemaful_formats_after_from_wire() {
        let input = "CREATE CODEC notification_codec FROM WIRE ";
        let suggestions = suggest_create_codec(input, input.len());

        assert!(suggestions.contains(&"JSON".to_string()));
        assert!(suggestions.contains(&"AVRO".to_string()));
        assert!(suggestions.contains(&"CBOR".to_string()));
    }

    #[test]
    fn suggests_wire_and_jaq_native_formats_after_from() {
        let input = "CREATE CODEC notification_codec FROM ";
        let suggestions = suggest_create_codec(input, input.len());

        assert!(suggestions.contains(&"WIRE".to_string()));
        assert!(suggestions.contains(&"JSON".to_string()));
        assert!(suggestions.contains(&"YAML".to_string()));
        assert!(suggestions.contains(&"TOML".to_string()));
        assert!(suggestions.contains(&"XML".to_string()));
        assert!(suggestions.contains(&"CBOR".to_string()));
        assert!(suggestions.contains(&"PROTOBUF".to_string()));
    }

    #[test]
    fn suggests_using_after_from_protobuf() {
        let input = "CREATE CODEC notification_codec FROM PROTOBUF ";
        let suggestions = suggest_create_codec(input, input.len());

        assert!(suggestions.contains(&"USING".to_string()));
        assert!(!suggestions.contains(&"WIRE".to_string()));
    }

    #[test]
    fn suggests_config_after_protobuf_resource_version() {
        let input =
            "CREATE CODEC notification_codec FROM PROTOBUF USING RESOURCE proto_bundle VERSION 1 ";
        let suggestions = suggest_create_codec(input, input.len());

        assert!(suggestions.contains(&"CONFIG".to_string()));
        assert!(!suggestions.contains(&"MESSAGE".to_string()));
    }

    #[test]
    fn suggests_on_after_with_jaq_transformations() {
        let input = "CREATE CODEC notification_codec FROM XML TO SCHEMA notification_schema WITH \
                     JAQ TRANSFORMATIONS ";
        let suggestions = suggest_create_codec(input, input.len());

        assert!(suggestions.contains(&"ON".to_string()));
        assert!(!suggestions.contains(&"TO".to_string()));
    }
}
