//! Codec parser and completion contract tests.

use meticulous::ResultExt as _;
use nervix_models::{
    ClientConfigEntry, CodecJaqFormat, CodecJaqTransformations, CodecProtobufConfig,
    CodecWireFormat, FieldName, WireSchemaName,
};
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
    "CREATE JAQ XML CODEC notification_codec TO SCHEMA notification_schema WITH JAQ \
     TRANSFORMATIONS ON INGESTION \".payload\" ON EMITTING \"{payload: {user_id}}\";",
    CodecJaqFormat::Xml,
    Some(".payload"),
    Some("{payload: {user_id}}")
)]
#[case::yaml_with_ingestion_transformation(
    "CREATE JAQ YAML CODEC notification_codec TO SCHEMA notification_schema WITH JAQ \
     TRANSFORMATIONS ON INGESTION \".payload\";",
    CodecJaqFormat::Yaml,
    Some(".payload"),
    None
)]
#[case::json_with_emitting_transformation(
    "CREATE JAQ JSON CODEC notification_codec TO SCHEMA notification_schema WITH JAQ \
     TRANSFORMATIONS ON EMITTING \"{payload: .}\";",
    CodecJaqFormat::Json,
    None,
    Some("{payload: .}")
)]
#[case::cbor_with_ingestion_transformation(
    "CREATE JAQ CBOR CODEC notification_codec TO SCHEMA notification_schema WITH JAQ \
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
        "CREATE PROTOBUF CODEC notification_codec USING RESOURCE proto_bundle VERSION 2 CONFIG \
         {\"file\" = \"notification.proto\", \"include\" = \".\"} MESSAGE \
         \"nervix.test.Notification\" TO SCHEMA notification_schema WITH JAQ TRANSFORMATIONS ON \
         INGESTION \".\" ON EMITTING \".\";",
    );
    let parsed = parse_create_codec_tokens(&tokens).expect("parse should succeed");

    assert_eq!(
        parsed.wire_format,
        CodecWireFormat::Protobuf(CodecProtobufConfig {
            resource: nervix_models::ResourceName::parse("proto_bundle").expect("valid identifier"),
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
        "CREATE PROTOBUF CODEC notification_codec USING RESOURCE proto_bundle VERSION LATEST \
         CONFIG {'file' = 'notification.proto'} MESSAGE 'nervix.test.Notification' TO SCHEMA \
         notification_schema WITH JAQ TRANSFORMATIONS ON INGESTION '.';",
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
    "CREATE WIRE JSON CODEC notification_codec FROM WIRE SCHEMA notification_wire TO SCHEMA \
     notification_schema;",
    CodecWireFormat::Json { wire_schema: wire_schema_name("notification_wire") },
    "notification_codec",
    "notification_schema"
)]
#[case::avro(
    "CREATE WIRE AVRO CODEC notification_codec FROM WIRE SCHEMA notification_wire TO SCHEMA \
     notification_schema;",
    CodecWireFormat::Avro { wire_schema: wire_schema_name("notification_wire") },
    "notification_codec",
    "notification_schema"
)]
#[case::cbor(
    "CREATE WIRE CBOR CODEC notification_codec FROM WIRE SCHEMA notification_wire TO SCHEMA \
     notification_schema;",
    CodecWireFormat::Cbor { wire_schema: wire_schema_name("notification_wire") },
    "notification_codec",
    "notification_schema"
)]
#[case::syslog(
    "CREATE SYSLOG CODEC events TO SCHEMA syslog_event;",
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
    "CREATE JAQ YAML CODEC notification_codec TO SCHEMA notification_schema WITH JAQ \
     TRANSFORMATION \".payload\";"
)]
#[case::syslog_jaq_transformation(
    "CREATE SYSLOG CODEC events TO SCHEMA syslog_event WITH JAQ TRANSFORMATIONS ON INGESTION '.';"
)]
#[case::syslog_encoding_rule(
    "CREATE SYSLOG CODEC events TO SCHEMA syslog_event ENCODE timestamp AS RFC3339;"
)]
#[case::incomplete_encoding_rule(
    "CREATE WIRE JSON CODEC orders_codec FROM WIRE SCHEMA orders_wire TO SCHEMA orders ENCODE \
     created_at RFC3339;"
)]
#[case::jaq_native_without_transformation(
    "CREATE JAQ XML CODEC notification_codec TO SCHEMA notification_schema;"
)]
#[case::protobuf_without_jaq_transformation(
    "CREATE PROTOBUF CODEC notification_codec USING RESOURCE proto_bundle VERSION 1 CONFIG \
     {\"file\" = \"notification.proto\"} MESSAGE \"nervix.test.Notification\" TO SCHEMA \
     notification_schema;"
)]
#[case::protobuf_without_config_clause(
    "CREATE PROTOBUF CODEC notification_codec USING RESOURCE proto_bundle VERSION 1 MESSAGE \
     \"nervix.test.Notification\" TO SCHEMA notification_schema WITH JAQ TRANSFORMATIONS ON \
     INGESTION \".\";"
)]
#[case::protobuf_without_resource_version(
    "CREATE PROTOBUF CODEC notification_codec USING RESOURCE proto_bundle CONFIG {\"file\" = \
     \"notification.proto\"} MESSAGE \"nervix.test.Notification\" TO SCHEMA notification_schema \
     WITH JAQ TRANSFORMATIONS ON INGESTION \".\";"
)]
#[case::protobuf_with_a_named_version(
    "CREATE PROTOBUF CODEC notification_codec USING RESOURCE proto_bundle VERSION newest CONFIG \
     {\"file\" = \"notification.proto\"} MESSAGE \"nervix.test.Notification\" TO SCHEMA \
     notification_schema WITH JAQ TRANSFORMATIONS ON INGESTION \".\";"
)]
#[case::jaq_transformations_without_direction(
    "CREATE JAQ XML CODEC notification_codec TO SCHEMA notification_schema WITH JAQ \
     TRANSFORMATIONS \".payload\";"
)]
#[case::schemaful_with_jaq_transformation(
    "CREATE WIRE JSON CODEC notification_codec FROM WIRE SCHEMA notification_wire TO SCHEMA \
     notification_schema WITH JAQ TRANSFORMATIONS ON INGESTION \".payload\";"
)]
#[case::batch_transformation_without_emitting(
    "CREATE JAQ JSON CODEC notification_codec TO SCHEMA notification_schema WITH JAQ \
     TRANSFORMATIONS ON EMITTING BATCH \".\";"
)]
#[case::batch_transformation_after_ingestion_only(
    "CREATE JAQ JSON CODEC notification_codec TO SCHEMA notification_schema WITH JAQ \
     TRANSFORMATIONS ON INGESTION \".\" ON EMITTING BATCH \".\";"
)]
#[case::batch_transformation_before_emitting(
    "CREATE JAQ JSON CODEC notification_codec TO SCHEMA notification_schema WITH JAQ \
     TRANSFORMATIONS ON EMITTING BATCH \".\" ON EMITTING \".\";"
)]
#[case::batch_message_before_message(
    "CREATE PROTOBUF CODEC notification_codec USING RESOURCE proto_bundle VERSION 1 CONFIG \
     {\"file\" = \"notification.proto\"} BATCH MESSAGE \"nervix.test.Batch\" MESSAGE \
     \"nervix.test.Notification\" TO SCHEMA notification_schema WITH JAQ TRANSFORMATIONS ON \
     EMITTING \".\";"
)]
#[case::batch_message_on_a_jaq_native_codec(
    "CREATE JAQ JSON CODEC notification_codec BATCH MESSAGE \"nervix.test.Batch\" TO SCHEMA \
     notification_schema WITH JAQ TRANSFORMATIONS ON EMITTING \".\";"
)]
fn rejects_invalid_codec_forms(#[case] input: &str) {
    assert!(parse_create_codec(input).is_err());
}

#[test]
fn parses_base64_field_encoding() {
    for format in ["JSON", "AVRO"] {
        for spelling in ["BASE64", "base64", "BaSe64"] {
            let input = format!(
                "CREATE WIRE {format} CODEC binary_codec FROM WIRE SCHEMA binary_wire TO SCHEMA \
                 binary_schema ENCODE payload AS {spelling};"
            );
            let parsed = parse_create_codec(&input).assured("BASE64 is a field encoding");
            assert_eq!(
                parsed.encoding_rules,
                vec![CodecEncodingRule {
                    field: FieldName::parse("payload").assured("valid field name"),
                    encoding: CodecEncoding::Base64,
                }]
            );
        }
    }
    let prefix = "CREATE WIRE JSON CODEC binary_codec FROM WIRE SCHEMA binary_wire TO SCHEMA \
                  binary_schema ENCODE payload AS ";
    let suggestions = suggest_create_codec(prefix, prefix.len());
    assert!(suggestions.contains(&"BASE64".to_string()));
    assert!(suggestions.contains(&"RFC3339".to_string()));
}

#[test]
fn parses_create_codec_with_rfc3339_datetime_encoding() {
    let tokens = to_tokens(
        "CREATE WIRE JSON CODEC orders_codec FROM WIRE SCHEMA orders_wire TO SCHEMA orders ENCODE \
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
    "CREATE WIRE JSON CODEC orders_codec FROM WIRE SCHEMA orders_wire TO SCHEMA orders EN",
    &["ENCODE"],
    &[]
)]
#[case::from_after_codec_name(
    "CREATE WIRE JSON CODEC notification_codec ",
    &["FROM"],
    &[]
)]
#[case::syslog_stays_on_its_codec_branch(
    "CREATE SYSLOG CODEC events ",
    &["TO"],
    &["USING", "WITH", "ENCODE"]
)]
#[case::using_after_protobuf_head(
    "CREATE PROTOBUF CODEC notification_codec ",
    &["USING"],
    &["WIRE"]
)]
#[case::only_version_after_protobuf_resource(
    "CREATE PROTOBUF CODEC notification_codec USING RESOURCE proto_bundle ",
    &["VERSION"],
    &["CONFIG", "MESSAGE"]
)]
#[case::latest_and_completed_version_after_protobuf_version(
    "CREATE PROTOBUF CODEC notification_codec USING RESOURCE proto_bundle VERSION ",
    &["LATEST", "completed_resource_version"],
    &["CONFIG", "integer_literal"]
)]
#[case::config_after_protobuf_latest_resource_version(
    "CREATE PROTOBUF CODEC notification_codec USING RESOURCE proto_bundle VERSION LATEST ",
    &["CONFIG"],
    &["MESSAGE", "LATEST"]
)]
#[case::config_after_protobuf_resource_version(
    "CREATE PROTOBUF CODEC notification_codec USING RESOURCE proto_bundle VERSION 1 ",
    &["CONFIG"],
    &["MESSAGE"]
)]
#[case::on_after_with_jaq_transformations(
    "CREATE JAQ XML CODEC notification_codec TO SCHEMA notification_schema WITH JAQ \
     TRANSFORMATIONS ",
    &["ON"],
    &["TO"]
)]
#[case::both_directions_after_with_jaq_transformations_on(
    "CREATE JAQ XML CODEC notification_codec TO SCHEMA notification_schema WITH JAQ \
     TRANSFORMATIONS ON ",
    &["INGESTION", "EMITTING"],
    &["TRANSFORMATIONS"]
)]
#[case::only_emitting_as_second_jaq_transformation_direction(
    "CREATE JAQ XML CODEC notification_codec TO SCHEMA notification_schema WITH JAQ \
     TRANSFORMATIONS ON INGESTION \".\" ON ",
    &["EMITTING"],
    &["INGESTION"]
)]
#[case::emitting_batch_after_emitting_transformation(
    "CREATE JAQ JSON CODEC notification_codec TO SCHEMA notification_schema WITH JAQ \
     TRANSFORMATIONS ON EMITTING \".\" ON EMITTING ",
    &["BATCH"],
    &["INGESTION"]
)]
#[case::emitting_after_ingestion_and_emitting_transformations(
    "CREATE JAQ JSON CODEC notification_codec TO SCHEMA notification_schema WITH JAQ \
     TRANSFORMATIONS ON INGESTION \".\" ON EMITTING \".\" ON ",
    &["EMITTING"],
    &["INGESTION"]
)]
#[case::batch_message_after_protobuf_message(
    "CREATE PROTOBUF CODEC notification_codec USING RESOURCE proto_bundle VERSION 1 \
     CONFIG {\"file\" = \"notification.proto\"} MESSAGE \"nervix.test.Notification\" ",
    &["BATCH MESSAGE", "TO"],
    &["BATCH", "WITH"]
)]
#[case::only_explicit_transformations_after_with_jaq(
    "CREATE JAQ XML CODEC notification_codec TO SCHEMA notification_schema WITH JAQ ",
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
            "CREATE JAQ JSON CODEC notification_codec TO SCHEMA notification_schema WITH JAQ \
             TRANSFORMATIONS ON EMITTING \"{id: .user_id}\" ON EMITTING BATCH \"{records: .}\";",
            None,
        ),
        (
            "CREATE JAQ JSON CODEC notification_codec TO SCHEMA notification_schema WITH JAQ \
             TRANSFORMATIONS ON INGESTION \".\" ON EMITTING \"{id: .user_id}\" ON EMITTING BATCH \
             \"{records: .}\";",
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
        "CREATE PROTOBUF CODEC notification_codec USING RESOURCE proto_bundle VERSION 1 CONFIG \
         {\"file\" = \"notification.proto\"} MESSAGE \"nervix.test.Notification\" BATCH MESSAGE \
         \"nervix.test.NotificationBatch\" TO SCHEMA notification_schema WITH JAQ TRANSFORMATIONS \
         ON EMITTING \".\";",
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
