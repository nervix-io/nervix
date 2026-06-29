use chumsky::prelude::*;
use nervix_models::{ModelKind, ShowCreate};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, ParseFromSourceError, client_ref, codec_ref, correlator_ref,
        current_word_prefix, deduplicator_ref, emitter_ref, endpoint_ref, generator_ref,
        inferencer_ref, ingestor_ref, into_parse_error, junction_ref, kw, kw_phrase2, lex_input,
        lookup_ref, reingestor_ref, relay_ref, reorderer_ref, schema_ref, suggestions_from_errors,
        tok, vhost_ref, window_processor_ref, wire_schema_ref,
    },
};

pub fn show_create_parser<'src>()
-> impl Parser<'src, &'src [Token], ShowCreate, extra::Err<ParseError<'src>>> + Clone {
    let target = choice((
        kw(Identifier::Schema)
            .ignore_then(schema_ref())
            .map(|name| ShowCreate {
                kind: ModelKind::Schema,
                name,
            }),
        kw(Identifier::Wire)
            .ignore_then(kw(Identifier::Schema))
            .ignore_then(wire_schema_ref())
            .map(|name| ShowCreate {
                kind: ModelKind::WireSchema,
                name,
            }),
        kw(Identifier::Codec)
            .ignore_then(codec_ref())
            .map(|name| ShowCreate {
                kind: ModelKind::Codec,
                name,
            }),
        kw(Identifier::Client)
            .ignore_then(client_ref())
            .map(|name| ShowCreate {
                kind: ModelKind::Client,
                name,
            }),
        kw(Identifier::Vhost)
            .ignore_then(vhost_ref())
            .map(|name| ShowCreate {
                kind: ModelKind::Vhost,
                name,
            }),
        kw(Identifier::Endpoint)
            .ignore_then(endpoint_ref())
            .map(|name| ShowCreate {
                kind: ModelKind::Endpoint,
                name,
            }),
        kw(Identifier::Generator)
            .ignore_then(generator_ref())
            .map(|name| ShowCreate {
                kind: ModelKind::Generator,
                name,
            }),
        kw(Identifier::Ingestor)
            .ignore_then(ingestor_ref())
            .map(|name| ShowCreate {
                kind: ModelKind::Ingestor,
                name,
            }),
        kw(Identifier::Reingestor)
            .ignore_then(reingestor_ref())
            .map(|name| ShowCreate {
                kind: ModelKind::Reingestor,
                name,
            }),
        kw(Identifier::Reorderer)
            .ignore_then(reorderer_ref())
            .map(|name| ShowCreate {
                kind: ModelKind::Reorderer,
                name,
            }),
        kw(Identifier::Inferencer)
            .ignore_then(inferencer_ref())
            .map(|name| ShowCreate {
                kind: ModelKind::Inferencer,
                name,
            }),
        kw(Identifier::Relay)
            .ignore_then(relay_ref())
            .map(|name| ShowCreate {
                kind: ModelKind::Relay,
                name,
            }),
        kw_phrase2(Identifier::Hash, Identifier::Map)
            .ignore_then(lookup_ref())
            .map(|name| ShowCreate {
                kind: ModelKind::Lookup,
                name,
            }),
        kw(Identifier::Junction)
            .ignore_then(junction_ref())
            .map(|name| ShowCreate {
                kind: ModelKind::Junction,
                name,
            }),
        kw(Identifier::Deduplicator)
            .ignore_then(deduplicator_ref())
            .map(|name| ShowCreate {
                kind: ModelKind::Deduplicator,
                name,
            }),
        kw(Identifier::Correlator)
            .ignore_then(correlator_ref())
            .map(|name| ShowCreate {
                kind: ModelKind::Correlator,
                name,
            }),
        kw_phrase2(Identifier::Window, Identifier::Processor)
            .ignore_then(window_processor_ref())
            .map(|name| ShowCreate {
                kind: ModelKind::WindowProcessor,
                name,
            }),
        kw(Identifier::Emitter)
            .ignore_then(emitter_ref())
            .map(|name| ShowCreate {
                kind: ModelKind::Emitter,
                name,
            }),
    ));

    kw(Identifier::Show)
        .ignore_then(kw(Identifier::Create))
        .ignore_then(target)
        .then_ignore(tok(Token::Semicolon).or_not())
}

pub fn parse_show_create_tokens(tokens: &[Token]) -> Result<ShowCreate, Vec<ParseError<'_>>> {
    let out = show_create_parser().then_ignore(end()).parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .expect("successful parse must have output"))
    }
}

pub fn parse_show_create(input: &str) -> Result<ShowCreate, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_show_create_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_show_create(input: &str, cursor: usize) -> Vec<String> {
    let safe_cursor = cursor.min(input.len());
    let prefix_src = &input[..safe_cursor];
    let prefix = current_word_prefix(prefix_src);

    let (_, _, tokens) = match lex_input(prefix_src) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    let out = show_create_parser()
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
    fn parses_show_create_lookup() {
        let tokens = to_tokens("SHOW CREATE HASH MAP zip_codes;");
        let parsed = parse_show_create_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.kind, ModelKind::Lookup);
        assert_eq!(parsed.name.as_str(), "zip_codes");
    }

    #[test]
    fn suggests_entity_keywords_after_show_create() {
        let input = "SHOW CREATE ";
        let suggestions = suggest_show_create(input, input.len());
        assert!(suggestions.contains(&"SCHEMA".to_string()));
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
        assert!(suggestions.contains(&"EMITTER".to_string()));
        assert!(!suggestions.contains(&"JSON".to_string()));
        assert!(!suggestions.contains(&"AVRO".to_string()));
    }
}
