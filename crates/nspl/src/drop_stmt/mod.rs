use chumsky::prelude::*;
use nervix_models::{DropModel, DropNode, ModelKind};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, ParseFromSourceError, client_ref, codec_ref, correlator_ref,
        current_word_prefix, deduplicator_ref, emitter_ref, endpoint_ref, inferencer_ref,
        ingestor_ref, into_parse_error, kw, lex_input, node_id, reingestor_ref, relay_ref,
        reorderer_ref, router_ref, schema_ref, suggestions_from_errors, tok, unifier_ref,
        vhost_ref, wire_schema_ref,
    },
};

pub fn drop_parser<'src>()
-> impl Parser<'src, &'src [Token], DropModel, extra::Err<ParseError<'src>>> + Clone {
    let target = choice((
        kw(Identifier::Schema)
            .ignore_then(schema_ref())
            .map(|name| DropModel {
                kind: ModelKind::Schema,
                name,
            }),
        kw(Identifier::Wire)
            .ignore_then(kw(Identifier::Schema))
            .ignore_then(wire_schema_ref())
            .map(|name| DropModel {
                kind: ModelKind::WireSchema,
                name,
            }),
        kw(Identifier::Codec)
            .ignore_then(codec_ref())
            .map(|name| DropModel {
                kind: ModelKind::Codec,
                name,
            }),
        kw(Identifier::Client)
            .ignore_then(client_ref())
            .map(|name| DropModel {
                kind: ModelKind::Client,
                name,
            }),
        kw(Identifier::Vhost)
            .ignore_then(vhost_ref())
            .map(|name| DropModel {
                kind: ModelKind::Vhost,
                name,
            }),
        kw(Identifier::Endpoint)
            .ignore_then(endpoint_ref())
            .map(|name| DropModel {
                kind: ModelKind::Endpoint,
                name,
            }),
        kw(Identifier::Ingestor)
            .ignore_then(ingestor_ref())
            .map(|name| DropModel {
                kind: ModelKind::Ingestor,
                name,
            }),
        kw(Identifier::Reingestor)
            .ignore_then(reingestor_ref())
            .map(|name| DropModel {
                kind: ModelKind::Reingestor,
                name,
            }),
        kw(Identifier::Router)
            .ignore_then(router_ref())
            .map(|name| DropModel {
                kind: ModelKind::Router,
                name,
            }),
        kw(Identifier::Reorderer)
            .ignore_then(reorderer_ref())
            .map(|name| DropModel {
                kind: ModelKind::Reorderer,
                name,
            }),
        kw(Identifier::Inferencer)
            .ignore_then(inferencer_ref())
            .map(|name| DropModel {
                kind: ModelKind::Inferencer,
                name,
            }),
        kw(Identifier::Relay)
            .ignore_then(relay_ref())
            .map(|name| DropModel {
                kind: ModelKind::Relay,
                name,
            }),
        kw(Identifier::Unifier)
            .ignore_then(unifier_ref())
            .map(|name| DropModel {
                kind: ModelKind::Unifier,
                name,
            }),
        kw(Identifier::Deduplicator)
            .ignore_then(deduplicator_ref())
            .map(|name| DropModel {
                kind: ModelKind::Deduplicator,
                name,
            }),
        kw(Identifier::Correlator)
            .ignore_then(correlator_ref())
            .map(|name| DropModel {
                kind: ModelKind::Correlator,
                name,
            }),
        kw(Identifier::Emitter)
            .ignore_then(emitter_ref())
            .map(|name| DropModel {
                kind: ModelKind::Emitter,
                name,
            }),
    ));

    kw(Identifier::Drop)
        .ignore_then(target)
        .then_ignore(tok(Token::Semicolon).or_not())
}

pub fn drop_node_parser<'src>()
-> impl Parser<'src, &'src [Token], DropNode, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Drop)
        .ignore_then(kw(Identifier::Node))
        .ignore_then(node_id())
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
            .expect("successful parse must have output"))
    }
}

pub fn parse_drop(input: &str) -> Result<DropModel, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_drop_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_drop(input: &str, cursor: usize) -> Vec<String> {
    let safe_cursor = cursor.min(input.len());
    let prefix_src = &input[..safe_cursor];
    let prefix = current_word_prefix(prefix_src);

    let (_, _, tokens) = match lex_input(prefix_src) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    let out = drop_parser().then_ignore(end()).parse(tokens.as_slice());
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
    fn parses_drop_schema() {
        let tokens = to_tokens("DROP SCHEMA event_schema;");
        let parsed = parse_drop_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.kind, ModelKind::Schema);
        assert_eq!(parsed.name.as_str(), "event_schema");
    }

    #[test]
    fn parses_drop_vhost() {
        let tokens = to_tokens("DROP VHOST edge;");
        let parsed = parse_drop_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.kind, ModelKind::Vhost);
        assert_eq!(parsed.name.as_str(), "edge");
    }

    #[test]
    fn parses_drop_unifier() {
        let tokens = to_tokens("DROP UNIFIER merge;");
        let parsed = parse_drop_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.kind, ModelKind::Unifier);
        assert_eq!(parsed.name.as_str(), "merge");
    }

    #[test]
    fn parses_drop_router() {
        let tokens = to_tokens("DROP ROUTER log_router;");
        let parsed = parse_drop_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.kind, ModelKind::Router);
        assert_eq!(parsed.name.as_str(), "log_router");
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
    fn parses_drop_node() {
        let tokens = to_tokens("DROP NODE node-2;");
        let parsed = drop_node_parser()
            .then_ignore(end())
            .parse(tokens.as_slice())
            .into_result()
            .expect("parse should succeed");
        assert_eq!(parsed.node_id, "node-2");
    }

    #[test]
    fn suggests_entity_keywords_after_drop() {
        let input = "DROP ";
        let suggestions = suggest_drop(input, input.len());
        assert!(suggestions.contains(&"SCHEMA".to_string()));
        assert!(suggestions.contains(&"WIRE".to_string()));
        assert!(suggestions.contains(&"CODEC".to_string()));
        assert!(suggestions.contains(&"CLIENT".to_string()));
        assert!(suggestions.contains(&"VHOST".to_string()));
        assert!(suggestions.contains(&"ENDPOINT".to_string()));
        assert!(suggestions.contains(&"INGESTOR".to_string()));
        assert!(suggestions.contains(&"ROUTER".to_string()));
        assert!(suggestions.contains(&"RELAY".to_string()));
        assert!(suggestions.contains(&"UNIFIER".to_string()));
        assert!(suggestions.contains(&"DEDUPLICATOR".to_string()));
        assert!(suggestions.contains(&"EMITTER".to_string()));
    }
}
