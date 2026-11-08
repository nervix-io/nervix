//! Creation-head completion through the composed public grammar.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** Full-phrase creation-head offers and their parser boundaries.
//! - **Depends on.** The production Plexus grammar and vocabulary.
//! - **Must not know.** Registry installation or runtime execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test harness stays outside product execution")
)]

use meticulous::ResultExt as _;
use nervix_models::{ModelKind, SemanticReference};
use nervix_plexus::client_statement::{
    ClientStatementGrammar, CompletionExpectation, suggest_client_expectations,
};
use rstest::rstest;

#[rstest]
#[case::paced("PACED DOMAIN")]
#[case::unpaced("UNPACED DOMAIN")]
#[case::json_schema("WIRE JSON SCHEMA")]
#[case::cbor_schema("WIRE CBOR SCHEMA")]
#[case::avro_schema("WIRE AVRO SCHEMA")]
#[case::signaling("SIGNALING PROTOCOL")]
#[case::window("WINDOW PROCESSOR")]
#[case::wasm("WASM PROCESSOR")]
#[case::hash_map("HASH MAP")]
fn creation_heads_are_composed(
    #[values("CREATE ", "CREATE IF NOT EXISTS ")] prefix: &str,
    #[case] head: &str,
) {
    let offered = suggest_client_expectations(prefix, prefix.len());
    assert!(
        offered.contains(&CompletionExpectation::Literal(head.to_string())),
        "{head} is missing after {prefix:?}: {offered:?}"
    );
}

#[rstest]
#[case::paced("PA", &["PACED DOMAIN"])]
#[case::unpaced("UNP", &["UNPACED DOMAIN"])]
#[case::wire("WIR", &["WIRE AVRO CODEC", "WIRE AVRO SCHEMA", "WIRE CBOR CODEC", "WIRE CBOR SCHEMA", "WIRE JSON CODEC", "WIRE JSON SCHEMA"])]
#[case::signaling("SIGN", &["SIGNALING PROTOCOL"])]
#[case::window("WIN", &["WINDOW PROCESSOR"])]
#[case::wasm("WAS", &["WASM PROCESSOR"])]
#[case::paced_domain("PACED DO", &["DOMAIN"])]
#[case::unpaced_domain("UNPACED DO", &["DOMAIN"])]
#[case::wire_format("WIRE J", &["JSON"])]
#[case::json_schema("WIRE JSON S", &["SCHEMA"])]
#[case::cbor_schema("WIRE CBOR S", &["SCHEMA"])]
#[case::avro_schema("WIRE AVRO S", &["SCHEMA"])]
#[case::signaling_protocol("SIGNALING PR", &["PROTOCOL"])]
#[case::window_processor("WINDOW PR", &["PROCESSOR"])]
#[case::wasm_processor("WASM PR", &["PROCESSOR"])]
fn partial_creation_heads_keep_their_branch(
    #[values("CREATE", "CREATE IF NOT EXISTS")] prefix: &str,
    #[case] partial: &str,
    #[case] expected: &[&str],
) {
    let input = format!("{prefix} {partial}");
    let offered = suggest_client_expectations(&input, input.len());
    let expected: Vec<_> = expected
        .iter()
        .map(|head| CompletionExpectation::Literal((*head).to_string()))
        .collect();
    assert_eq!(offered, expected, "{input}");
}

#[rstest]
#[case::paced("PACED DOMAIN", "domain_name")]
#[case::unpaced("UNPACED DOMAIN", "domain_name")]
#[case::json_schema("WIRE JSON SCHEMA", "wire_schema_name")]
#[case::cbor_schema("WIRE CBOR SCHEMA", "wire_schema_name")]
#[case::avro_schema("WIRE AVRO SCHEMA", "wire_schema_name")]
#[case::signaling("SIGNALING PROTOCOL", "signaling_protocol_name")]
#[case::window("WINDOW PROCESSOR", "window_processor_name")]
#[case::wasm("WASM PROCESSOR", "wasm_processor_name")]
fn complete_creation_heads_expect_only_their_name(
    #[values("CREATE", "CREATE IF NOT EXISTS")] prefix: &str,
    #[case] head: &str,
    #[case] name: &str,
) {
    let input = format!("{prefix} {head} ");
    let offered = ClientStatementGrammar::new().suggest_statement(&input, input.len());
    assert_eq!(offered, [name], "{input}");
}

#[rstest]
#[case::window_relay("WINDOW PROCESSOR p FROM ", SemanticReference::Model(ModelKind::Relay))]
#[case::wasm_relay("WASM PROCESSOR p FROM ", SemanticReference::Model(ModelKind::Relay))]
#[case::wasm_resource(
    "WASM PROCESSOR p FROM incoming USING RESOURCE ",
    SemanticReference::Resource
)]
#[case::signaling_resource(
    "SIGNALING PROTOCOL p FORMAT PROTOBUF USING RESOURCE ",
    SemanticReference::Resource
)]
fn creation_bodies_keep_their_reference_kind(
    #[values("CREATE", "CREATE IF NOT EXISTS")] prefix: &str,
    #[case] body: &str,
    #[case] reference: SemanticReference,
) {
    let input = format!("{prefix} {body}");
    assert_eq!(
        suggest_client_expectations(&input, input.len()),
        [CompletionExpectation::Semantic(reference)],
        "{input}"
    );
}

#[rstest]
#[case::paced("PACED DOMAIN live WITH PERIOD 1s SKEW 100ms")]
#[case::unpaced("UNPACED DOMAIN live")]
#[case::json_schema("WIRE JSON SCHEMA payload MODE STRICT (value NUMBER)")]
#[case::cbor_schema("WIRE CBOR SCHEMA payload MODE LOOSE (value BYTES)")]
#[case::avro_schema("WIRE AVRO SCHEMA payload MODE STRICT (value LONG)")]
#[case::signaling(
    "SIGNALING PROTOCOL handshake FORMAT JSON ON CONNECT SEND JAQ '{id: 1}' TIMEOUT 5s"
)]
#[case::window(
    "WINDOW PROCESSOR counted FROM incoming DETACHED WIDTH 3 MESSAGES STEP 3 MESSAGES UNBRANCHED \
     TO outgoing SET value = COUNT(input.value) ON MESSAGE ERROR LOG"
)]
#[case::wasm(
    "WASM PROCESSOR mapped FROM incoming DETACHED USING RESOURCE guest VERSION 1 FILE \
     'guest.wasm' MAX FUEL 1000 MAX MEMORY 1MiB UNBRANCHED TO outgoing SET value = value ON \
     MESSAGE ERROR LOG ON GLOBAL ERROR LOG"
)]
fn creation_heads_preserve_models_canonical_text_and_statement_spans(
    #[values("CREATE", "CREATE IF NOT EXISTS")] prefix: &str,
    #[case] body: &str,
) {
    let grammar = ClientStatementGrammar::new();
    let source = format!("{prefix} {body};");
    let parsed = grammar
        .parse_statement(&source)
        .assured("the current creation head and its body parse");
    let lowercase = grammar
        .parse_statement(&source.to_ascii_lowercase())
        .assured("keyword tokens accept lowercase spelling");
    assert_eq!(lowercase, parsed);
    let canonical = parsed
        .to_canonical_plexus()
        .assured("every current creation statement has canonical text");
    assert_eq!(
        grammar
            .parse_statement(&canonical)
            .assured("the same grammar reads its canonical creation text"),
        parsed
    );

    let batch_prefix = "// created after a comment\n ";
    let batch = format!("{batch_prefix}{source}\nCREATE DOMAIN next;");
    let statements = grammar
        .parse_sources(&batch)
        .assured("the composed grammar keeps each statement's source range");
    assert_eq!(statements.len(), 2);
    assert_eq!(statements[0].span.start, batch_prefix.len());
    assert_eq!(statements[0].source(&batch), source);
    assert_eq!(statements[0].statement, parsed);
}

#[rstest]
#[case::paced("PACED RELAY name")]
#[case::unpaced("UNPACED RELAY name")]
#[case::json_schema("WIRE JSON RELAY name")]
#[case::cbor_schema("WIRE CBOR RELAY name")]
#[case::avro_schema("WIRE AVRO RELAY name")]
#[case::signaling("SIGNALING RELAY name")]
#[case::window("WINDOW RELAY name")]
#[case::wasm("WASM RELAY name")]
fn malformed_creation_heads_report_the_wrong_token(
    #[values("CREATE", "CREATE IF NOT EXISTS")] prefix: &str,
    #[case] body: &str,
) {
    let source = format!("{prefix} {body};");
    let error = ClientStatementGrammar::new()
        .parse_statement(&source)
        .expect_err("RELAY cannot complete any of these multi-word creation heads");
    assert_eq!(error.current_context().source_text(), source);
    let diagnostics = error.current_context().diagnostics();
    assert!(!diagnostics.is_empty());
    for diagnostic in diagnostics {
        assert_eq!(&source[diagnostic.span.clone()], "RELAY");
    }
}
