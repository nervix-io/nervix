use chumsky::prelude::*;
use meticulous::OptionExt as _;
use nervix_models::ShowCreate;

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        LexedInput, ParseError, ParseFromSourceError, into_parse_error, kw, lex_input, model_ref,
        suggest_from, tok,
    },
};

pub fn show_create_parser<'src>()
-> impl Parser<'src, &'src [Token], ShowCreate, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Show)
        .ignore_then(kw(Identifier::Create))
        .ignore_then(model_ref())
        .map(|target| ShowCreate {
            kind: target.kind,
            name: target.identifier,
        })
        .then_ignore(tok(Token::Semicolon).or_not())
}

pub fn parse_show_create_tokens(tokens: &[Token]) -> Result<ShowCreate, Vec<ParseError<'_>>> {
    let out = show_create_parser().then_ignore(end()).parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .verified("has_errors returned false above, so this parse produced output"))
    }
}

pub fn parse_show_create(input: &str) -> error_stack::Result<ShowCreate, ParseFromSourceError> {
    let LexedInput {
        source,
        spanned_tokens,
        tokens,
    } = lex_input(input)?;
    parse_show_create_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_show_create(input: &str, cursor: usize) -> Vec<String> {
    suggest_from!(input, cursor, show_create_parser())
}

#[cfg(test)]
mod tests {
    use nervix_models::ModelKind;

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
    fn parses_show_create_transport() {
        let tokens = to_tokens("SHOW CREATE CLIENT kafka_main;");
        let parsed = parse_show_create_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.kind, ModelKind::Client);
        assert_eq!(parsed.name.as_str(), "kafka_main");
    }

    #[test]
    fn parses_show_create_emitter() {
        let tokens = to_tokens("SHOW CREATE EMITTER e;");
        let parsed = parse_show_create_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.kind, ModelKind::Emitter);
        assert_eq!(parsed.name.as_str(), "e");
    }

    #[test]
    fn parses_and_completes_show_create_branch() {
        let parsed = parse_show_create("SHOW CREATE BRANCH by_tenant;")
            .expect("a named branch has a SHOW CREATE form");
        assert_eq!(parsed.kind, ModelKind::Branch);
        assert_eq!(parsed.name.as_str(), "by_tenant");
        assert!(parse_show_create("SHOW CREATE BRANCH;").is_err());
        let input = "SHOW CREATE BRANCH ";
        assert_eq!(
            suggest_show_create(input, input.len()),
            vec!["ref:branch".to_string()]
        );
    }

    #[test]
    fn parses_show_create_junction() {
        let tokens = to_tokens("SHOW CREATE JUNCTION merge;");
        let parsed = parse_show_create_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.kind, ModelKind::Junction);
        assert_eq!(parsed.name.as_str(), "merge");
    }

    #[test]
    fn parses_show_create_deduplicator() {
        let tokens = to_tokens("SHOW CREATE DEDUPLICATOR dedup;");
        let parsed = parse_show_create_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.kind, ModelKind::Deduplicator);
        assert_eq!(parsed.name.as_str(), "dedup");
    }

    #[test]
    fn parses_show_create_generator() {
        let tokens = to_tokens("SHOW CREATE GENERATOR clock;");
        let parsed = parse_show_create_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.kind, ModelKind::Generator);
        assert_eq!(parsed.name.as_str(), "clock");
    }

    #[test]
    fn parses_show_create_window_processor() {
        let tokens = to_tokens("SHOW CREATE WINDOW PROCESSOR latency;");
        let parsed = parse_show_create_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.kind, ModelKind::WindowProcessor);
        assert_eq!(parsed.name.as_str(), "latency");
    }

    #[test]
    fn parses_show_create_signaling_protocol() {
        let parsed = parse_show_create("SHOW CREATE SIGNALING PROTOCOL protobuf_subscribe;")
            .expect("parse should succeed");
        assert_eq!(parsed.kind, ModelKind::SignalingProtocol);
        assert_eq!(parsed.name.as_str(), "protobuf_subscribe");
    }

    #[test]
    fn parses_show_create_wasm_processor() {
        let parsed = parse_show_create("SHOW CREATE WASM PROCESSOR filter_even_rows;")
            .expect("parse should succeed");
        assert_eq!(parsed.kind, ModelKind::WasmProcessor);
        assert_eq!(parsed.name.as_str(), "filter_even_rows");
    }

    #[test]
    fn rejects_show_create_with_a_partial_binding_kind_phrase() {
        assert!(parse_show_create("SHOW CREATE SIGNALING protobuf_subscribe;").is_err());
        assert!(parse_show_create("SHOW CREATE WASM filter_even_rows;").is_err());
    }

    #[test]
    fn completes_binding_kind_references_without_leaking_other_kinds() {
        let input = "SHOW CREATE SIGNALING PROTOCOL ";
        let suggestions = suggest_show_create(input, input.len());
        assert_eq!(suggestions, vec!["ref:signaling_protocol".to_string()]);

        let input = "SHOW CREATE WASM PROCESSOR ";
        let suggestions = suggest_show_create(input, input.len());
        assert_eq!(suggestions, vec!["ref:wasm_processor".to_string()]);
    }

    #[test]
    fn rejects_show_create_without_entity_kind() {
        let tokens = to_tokens("SHOW CREATE latency;");
        assert!(parse_show_create_tokens(&tokens).is_err());
    }

    #[test]
    fn parses_show_create_vhost() {
        let tokens = to_tokens("SHOW CREATE VHOST edge;");
        let parsed = parse_show_create_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.kind, ModelKind::Vhost);
        assert_eq!(parsed.name.as_str(), "edge");
    }

    #[test]
    fn parses_show_create_endpoint() {
        let tokens = to_tokens("SHOW CREATE ENDPOINT my_ws;");
        let parsed = parse_show_create_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.kind, ModelKind::Endpoint);
        assert_eq!(parsed.name.as_str(), "my_ws");
    }

    #[test]
    fn parses_show_create_placement() {
        let parsed =
            parse_show_create("SHOW CREATE PLACEMENT critical;").expect("parse should succeed");
        assert_eq!(parsed.kind, ModelKind::Placement);
        assert_eq!(parsed.name.as_str(), "critical");
    }

    #[test]
    fn parses_show_create_lookup() {
        let tokens = to_tokens("SHOW CREATE HASH MAP zip_codes;");
        let parsed = parse_show_create_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.kind, ModelKind::Lookup);
        assert_eq!(parsed.name.as_str(), "zip_codes");
    }

    #[test]
    fn parses_exact_wire_schema_kind_and_rejects_generic_kind() {
        let parsed = parse_show_create("SHOW CREATE WIRE JSON SCHEMA payload;")
            .expect("parse should succeed");
        assert_eq!(parsed.kind, ModelKind::WireJsonSchema);
        assert_eq!(parsed.name.as_str(), "payload");

        assert!(parse_show_create("SHOW CREATE WIRE SCHEMA payload;").is_err());
    }

    #[test]
    fn completes_exact_wire_schema_kind() {
        let input = "SHOW CREATE WIRE ";
        let suggestions = suggest_show_create(input, input.len());
        for format in ["JSON", "CBOR", "AVRO"] {
            assert!(suggestions.contains(&format.to_string()));
        }
        assert!(!suggestions.contains(&"SCHEMA".to_string()));

        let input = "SHOW CREATE WIRE JSON ";
        let suggestions = suggest_show_create(input, input.len());
        assert_eq!(suggestions, vec!["SCHEMA".to_string()]);
    }

    #[test]
    fn suggests_entity_keywords_after_show_create() {
        let input = "SHOW CREATE ";
        let suggestions = suggest_show_create(input, input.len());
        assert!(suggestions.contains(&"SCHEMA".to_string()));
        assert!(suggestions.contains(&"BRANCH".to_string()));
        assert!(suggestions.contains(&"WIRE".to_string()));
        assert!(suggestions.contains(&"CODEC".to_string()));
        assert!(suggestions.contains(&"CLIENT".to_string()));
        assert!(suggestions.contains(&"VHOST".to_string()));
        assert!(suggestions.contains(&"ENDPOINT".to_string()));
        assert!(suggestions.contains(&"GENERATOR".to_string()));
        assert!(suggestions.contains(&"INGESTOR".to_string()));
        assert!(suggestions.contains(&"RELAY".to_string()));
        assert!(suggestions.contains(&"HASH MAP".to_string()));
        assert!(suggestions.contains(&"JUNCTION".to_string()));
        assert!(suggestions.contains(&"DEDUPLICATOR".to_string()));
        assert!(suggestions.contains(&"WINDOW PROCESSOR".to_string()));
        assert!(suggestions.contains(&"SIGNALING PROTOCOL".to_string()));
        assert!(suggestions.contains(&"WASM PROCESSOR".to_string()));
        assert!(suggestions.contains(&"EMITTER".to_string()));
        assert!(suggestions.contains(&"PLACEMENT".to_string()));
        assert!(suggestions.contains(&"UDF".to_string()));
        assert!(!suggestions.contains(&"JSON".to_string()));
        assert!(!suggestions.contains(&"AVRO".to_string()));
    }
}
