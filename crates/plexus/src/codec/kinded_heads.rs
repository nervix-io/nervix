//! Test harness: current codec heads and their family-specific completion boundaries.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** Parser and composed-session expectations for codec declarations.
//! - **Depends on.** The production Plexus grammar and vocabulary.
//! - **Must not know.** Registry installation or codec execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test harness stays outside product execution")
)]

use meticulous::ResultExt as _;
use nervix_models::{CodecWireFormat, ModelKind, SemanticReference};
use rstest::rstest;

use super::parse_create_codec;
use crate::client_statement::{CompletionExpectation, suggest_client_expectations};

#[rstest]
#[case::wire_json("WIRE JSON", "FROM WIRE SCHEMA source_wire")]
#[case::wire_cbor("WIRE CBOR", "FROM WIRE SCHEMA source_wire")]
#[case::wire_avro("WIRE AVRO", "FROM WIRE SCHEMA source_wire")]
#[case::jaq_json("JAQ JSON", "WITH JAQ TRANSFORMATIONS ON INGESTION '.'")]
#[case::jaq_yaml("JAQ YAML", "WITH JAQ TRANSFORMATIONS ON INGESTION '.'")]
#[case::jaq_toml("JAQ TOML", "WITH JAQ TRANSFORMATIONS ON INGESTION '.'")]
#[case::jaq_xml("JAQ XML", "WITH JAQ TRANSFORMATIONS ON INGESTION '.'")]
#[case::jaq_cbor("JAQ CBOR", "WITH JAQ TRANSFORMATIONS ON INGESTION '.'")]
#[case::syslog("SYSLOG", "")]
fn every_family_head_preserves_its_name_and_schema(#[case] head: &str, #[case] body: &str) {
    for prefix in ["CREATE", "CREATE IF NOT EXISTS"] {
        let source = if head.starts_with("WIRE ") {
            format!("{prefix} {head} CODEC payload_codec {body} TO SCHEMA payload;")
        } else {
            format!("{prefix} {head} CODEC payload_codec TO SCHEMA payload {body};")
        };
        let parsed =
            parse_create_codec(&source).assured("the current codec family has its own head");
        assert_eq!(parsed.name.as_str(), "payload_codec");
        assert_eq!(parsed.schema.as_str(), "payload");
        assert_eq!(parsed.if_not_exists, prefix == "CREATE IF NOT EXISTS");
        assert!(parsed.encoding_rules.is_empty());
        let parsed_head = match &parsed.wire_format {
            CodecWireFormat::Json { wire_schema } => {
                assert_eq!(wire_schema.as_str(), "source_wire");
                "WIRE JSON".to_string()
            }
            CodecWireFormat::Cbor { wire_schema } => {
                assert_eq!(wire_schema.as_str(), "source_wire");
                "WIRE CBOR".to_string()
            }
            CodecWireFormat::Avro { wire_schema } => {
                assert_eq!(wire_schema.as_str(), "source_wire");
                "WIRE AVRO".to_string()
            }
            CodecWireFormat::JaqNative {
                format,
                transformations,
            } => {
                assert_eq!(transformations.on_ingestion.as_deref(), Some("."));
                assert_eq!(transformations.on_emitting, None);
                assert_eq!(transformations.on_emitting_batch, None);
                format!("JAQ {}", format.as_ref())
            }
            CodecWireFormat::Syslog => "SYSLOG".to_string(),
            CodecWireFormat::Protobuf(_) => panic!("a non-Protobuf head selected Protobuf"),
        };
        assert_eq!(parsed_head, head);
    }
}

#[test]
fn protobuf_head_preserves_resource_binding_and_both_emitting_programs() {
    let parsed = parse_create_codec(
        "CREATE IF NOT EXISTS PROTOBUF CODEC payload_codec USING RESOURCE bundle VERSION LATEST \
         CONFIG {'file' = 'payload.proto'} MESSAGE 'test.Payload' BATCH MESSAGE 'test.Batch' TO \
         SCHEMA payload WITH JAQ TRANSFORMATIONS ON INGESTION '.record' ON EMITTING '{record: .}' \
         ON EMITTING BATCH '{records: .}';",
    )
    .assured("a protobuf codec keeps its complete resource and transformation body");
    let CodecWireFormat::Protobuf(config) = &parsed.wire_format else {
        panic!("the PROTOBUF head must select a protobuf codec");
    };
    assert_eq!(config.resource.as_str(), "bundle");
    assert_eq!(
        config.resource_version,
        nervix_models::RequestedResourceVersion::Latest
    );
    assert_eq!(config.config[0].key, "file");
    assert_eq!(config.config[0].value, "payload.proto");
    assert_eq!(config.message, "test.Payload");
    assert_eq!(config.batch_message.as_deref(), Some("test.Batch"));
    assert_eq!(
        config.transformations.on_ingestion.as_deref(),
        Some(".record")
    );
    assert_eq!(
        config.transformations.on_emitting.as_deref(),
        Some("{record: .}")
    );
    assert_eq!(
        config.transformations.on_emitting_batch.as_deref(),
        Some("{records: .}")
    );
}

#[rstest]
#[case::wire_without_schema("CREATE WIRE JSON CODEC c TO SCHEMA s;")]
#[case::wire_with_transform(
    "CREATE WIRE JSON CODEC c FROM WIRE SCHEMA w TO SCHEMA s WITH JAQ TRANSFORMATIONS ON \
     INGESTION '.';"
)]
#[case::jaq_with_wire_body("CREATE JAQ JSON CODEC c FROM WIRE SCHEMA w TO SCHEMA s;")]
#[case::jaq_without_transform("CREATE JAQ YAML CODEC c TO SCHEMA s;")]
#[case::syslog_with_encoding("CREATE SYSLOG CODEC c TO SCHEMA s ENCODE timestamp AS RFC3339;")]
#[case::syslog_with_transform(
    "CREATE SYSLOG CODEC c TO SCHEMA s WITH JAQ TRANSFORMATIONS ON INGESTION '.';"
)]
#[case::protobuf_without_resource(
    "CREATE PROTOBUF CODEC c TO SCHEMA s WITH JAQ TRANSFORMATIONS ON INGESTION '.';"
)]
fn family_bodies_do_not_cross_head_boundaries(#[case] source: &str) {
    assert!(parse_create_codec(source).is_err(), "{source}");
}

#[test]
fn session_completion_offers_every_codec_head_as_one_phrase() {
    for prefix in ["CREATE ", "CREATE IF NOT EXISTS "] {
        let offered = suggest_client_expectations(prefix, prefix.len());
        for head in [
            "WIRE JSON CODEC",
            "WIRE CBOR CODEC",
            "WIRE AVRO CODEC",
            "JAQ JSON CODEC",
            "JAQ YAML CODEC",
            "JAQ TOML CODEC",
            "JAQ XML CODEC",
            "JAQ CBOR CODEC",
            "PROTOBUF CODEC",
            "SYSLOG CODEC",
        ] {
            assert!(
                offered.contains(&CompletionExpectation::Literal(head.to_string())),
                "{head} is missing after {prefix:?}: {offered:?}"
            );
        }
    }
}

#[rstest]
#[case::json("JSON", ModelKind::WireJsonSchema)]
#[case::cbor("CBOR", ModelKind::WireCborSchema)]
#[case::avro("AVRO", ModelKind::WireAvroSchema)]
fn wire_schema_completion_is_selected_by_the_head(#[case] format: &str, #[case] kind: ModelKind) {
    let prefix = format!("CREATE WIRE {format} CODEC c FROM WIRE SCHEMA ");
    let offered = suggest_client_expectations(&prefix, prefix.len());
    assert_eq!(
        offered,
        vec![CompletionExpectation::Semantic(SemanticReference::Model(
            kind
        ))]
    );
}
