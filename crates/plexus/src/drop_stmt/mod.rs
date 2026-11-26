//! The `DROP` statement: removing one domain entity of any kind, and removing a node from the
//! cluster.
//!
//! Layer: language.
//! - **Owns.** The `DROP` grammar, its parse and completion entry points.
//! - **Depends on.** Shared language tokens and the composed kind-and-name reference every
//!   statement that addresses one domain entity shares.
//! - **Must not know.** Registry state, what a drop removes at runtime or how its answer reads.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use chumsky::prelude::*;
use meticulous::OptionExt as _;
use nervix_models::{DropModel, DropNode};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        LexedInput, ParseError, ParseFromSourceError, cluster_node_name, into_parse_error, kw,
        lex_input, model_ref, suggest_from, tok,
    },
};

/// `DROP <kind> <name>` for every kind of domain entity, each kind named by the composed keyword
/// phrase of its `CREATE` head, as every statement that addresses one entity names it.
pub fn drop_parser<'src>()
-> impl Parser<'src, &'src [Token], DropModel, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Drop)
        .ignore_then(model_ref())
        .map(|target| DropModel {
            kind: target.kind,
            name: target.identifier,
        })
        .then_ignore(tok(Token::Semicolon).or_not())
}

pub fn drop_node_parser<'src>()
-> impl Parser<'src, &'src [Token], DropNode, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Drop)
        .ignore_then(kw(Identifier::Node))
        .ignore_then(cluster_node_name())
        .map(|node_id| DropNode { node_id })
        .then_ignore(tok(Token::Semicolon).or_not())
}

pub fn parse_drop_tokens(tokens: &[Token]) -> Result<DropModel, Vec<ParseError<'_>>> {
    let out = drop_parser().then_ignore(end()).parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .verified("has_errors returned false above, so this parse produced output"))
    }
}

pub fn parse_drop(input: &str) -> error_stack::Result<DropModel, ParseFromSourceError> {
    let LexedInput {
        source,
        spanned_tokens,
        tokens,
    } = lex_input(input)?;
    parse_drop_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_drop(input: &str, cursor: usize) -> Vec<String> {
    suggest_from!(input, cursor, drop_parser())
}

#[cfg(test)]
mod tests {
    use nervix_models::{ClusterNodeName, ModelKind};

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
    fn parses_drop_schema() {
        let tokens = to_tokens("DROP SCHEMA event_schema;");
        let parsed = parse_drop_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.kind, ModelKind::Schema);
        assert_eq!(parsed.name.as_str(), "event_schema");
    }

    #[test]
    fn parses_exact_wire_schema_kind_and_rejects_generic_kind() {
        let parsed =
            parse_drop("DROP WIRE CBOR SCHEMA event_schema;").expect("parse should succeed");
        assert_eq!(parsed.kind, ModelKind::WireCborSchema);
        assert_eq!(parsed.name.as_str(), "event_schema");

        assert!(parse_drop("DROP WIRE SCHEMA event_schema;").is_err());
    }

    #[test]
    fn completes_exact_wire_schema_kind() {
        let input = "DROP WIRE ";
        let suggestions = suggest_drop(input, input.len());
        for format in ["JSON", "CBOR", "AVRO"] {
            assert!(suggestions.contains(&format.to_string()));
        }
        assert!(!suggestions.contains(&"SCHEMA".to_string()));

        let input = "DROP WIRE CBOR ";
        let suggestions = suggest_drop(input, input.len());
        assert_eq!(suggestions, vec!["SCHEMA".to_string()]);
    }

    #[test]
    fn parses_drop_vhost() {
        let tokens = to_tokens("DROP VHOST edge;");
        let parsed = parse_drop_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.kind, ModelKind::Vhost);
        assert_eq!(parsed.name.as_str(), "edge");
    }

    #[test]
    fn parses_drop_junction() {
        let tokens = to_tokens("DROP JUNCTION merge;");
        let parsed = parse_drop_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.kind, ModelKind::Junction);
        assert_eq!(parsed.name.as_str(), "merge");
    }

    #[test]
    fn parses_drop_deduplicator() {
        let tokens = to_tokens("DROP DEDUPLICATOR dedup;");
        let parsed = parse_drop_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.kind, ModelKind::Deduplicator);
        assert_eq!(parsed.name.as_str(), "dedup");
    }

    #[test]
    fn parses_drop_endpoint() {
        let tokens = to_tokens("DROP ENDPOINT my_ws;");
        let parsed = parse_drop_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.kind, ModelKind::Endpoint);
        assert_eq!(parsed.name.as_str(), "my_ws");
    }

    #[test]
    fn parses_drop_placement() {
        let parsed = parse_drop("DROP PLACEMENT critical;").expect("parse should succeed");
        assert_eq!(parsed.kind, ModelKind::Placement);
        assert_eq!(parsed.name.as_str(), "critical");
    }

    #[test]
    fn parses_drop_node() {
        let tokens = to_tokens("DROP NODE node-2;");
        let parsed = drop_node_parser()
            .then_ignore(end())
            .parse(tokens.as_slice())
            .into_result()
            .expect("parse should succeed");
        assert_eq!(
            parsed.node_id,
            ClusterNodeName::parse("node-2").expect("valid name")
        );
    }

    #[test]
    fn parses_a_drop_of_every_kind_its_create_head_names_with_a_phrase() {
        for (source, kind, name) in [
            (
                "DROP WINDOW PROCESSOR counted;",
                ModelKind::WindowProcessor,
                "counted",
            ),
            (
                "DROP WASM PROCESSOR counting_guest;",
                ModelKind::WasmProcessor,
                "counting_guest",
            ),
            (
                "DROP GENERATOR heartbeat;",
                ModelKind::Generator,
                "heartbeat",
            ),
            ("DROP HASH MAP zip_codes;", ModelKind::Lookup, "zip_codes"),
            (
                "DROP PARAMETRIZER by_tenant;",
                ModelKind::Parametrizer,
                "by_tenant",
            ),
            (
                "DROP SIGNALING PROTOCOL protobuf_subscribe;",
                ModelKind::SignalingProtocol,
                "protobuf_subscribe",
            ),
        ] {
            let parsed = parse_drop(source).expect("parse should succeed");
            assert_eq!(parsed.kind, kind, "{source}");
            assert_eq!(parsed.name.as_str(), name, "{source}");
        }
    }

    #[test]
    fn rejects_a_drop_with_a_partial_kind_phrase() {
        for source in [
            "DROP WINDOW counted;",
            "DROP PROCESSOR counted;",
            "DROP WASM counting_guest;",
            "DROP HASH zip_codes;",
            "DROP MAP zip_codes;",
            "DROP SIGNALING protobuf_subscribe;",
            "DROP PROTOCOL protobuf_subscribe;",
        ] {
            assert!(parse_drop(source).is_err(), "{source} must not parse");
        }
    }

    #[test]
    fn suggests_every_entity_kind_after_drop() {
        let input = "DROP ";
        let suggestions = suggest_drop(input, input.len());
        for phrase in [
            "SCHEMA",
            "PARAMETRIZER",
            "WIRE JSON SCHEMA",
            "WIRE CBOR SCHEMA",
            "WIRE AVRO SCHEMA",
            "CODEC",
            "CLIENT",
            "VHOST",
            "ENDPOINT",
            "SIGNALING PROTOCOL",
            "GENERATOR",
            "INGESTOR",
            "REINGESTOR",
            "REORDERER",
            "INFERENCER",
            "WASM PROCESSOR",
            "RELAY",
            "HASH MAP",
            "JUNCTION",
            "DEDUPLICATOR",
            "CORRELATOR",
            "WINDOW PROCESSOR",
            "EMITTER",
            "PLACEMENT",
            "UDF",
        ] {
            assert!(
                suggestions.contains(&phrase.to_string()),
                "{phrase} missing from {suggestions:?}"
            );
        }
        // A phrase completes as one item, never as its first word alone.
        for word in ["WINDOW", "WASM", "HASH", "SIGNALING", "PROCESSOR", "MAP"] {
            assert!(
                !suggestions.contains(&word.to_string()),
                "{word} leaked into {suggestions:?}"
            );
        }
    }

    #[test]
    fn completes_the_reference_of_each_phrase_kind_without_leaking_other_kinds() {
        for (input, reference) in [
            ("DROP WINDOW PROCESSOR ", "ref:window_processor"),
            ("DROP WASM PROCESSOR ", "ref:wasm_processor"),
            ("DROP GENERATOR ", "ref:generator"),
            ("DROP HASH MAP ", "ref:lookup"),
            ("DROP PARAMETRIZER ", "ref:parametrizer"),
            ("DROP SIGNALING PROTOCOL ", "ref:signaling_protocol"),
        ] {
            let suggestions = suggest_drop(input, input.len());
            assert_eq!(suggestions, vec![reference.to_string()], "{input}");
        }
    }
}
