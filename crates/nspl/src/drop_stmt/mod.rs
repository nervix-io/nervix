use chumsky::prelude::*;
use meticulous::OptionExt as _;
use nervix_models::{DropModel, DropNode, ModelKind};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        LexedInput, ParseError, ParseFromSourceError, boxed_choice, client_ref, cluster_node_name,
        codec_ref, correlator_ref, deduplicator_ref, emitter_ref, endpoint_ref, inferencer_ref,
        ingestor_ref, into_parse_error, junction_ref, kw, lex_input, placement_ref, reingestor_ref,
        relay_ref, reorderer_ref, schema_ref, suggest_from, tok, udf_ref, vhost_ref,
        wire_avro_schema_ref, wire_cbor_schema_ref, wire_json_schema_ref,
    },
};

pub fn drop_parser<'src>()
-> impl Parser<'src, &'src [Token], DropModel, extra::Err<ParseError<'src>>> + Clone {
    let target = boxed_choice!(
        kw(Identifier::Schema)
            .ignore_then(schema_ref())
            .map(|name| DropModel {
                kind: ModelKind::Schema,
                name: name.into(),
            }),
        kw(Identifier::Wire)
            .ignore_then(kw(Identifier::Json))
            .ignore_then(kw(Identifier::Schema))
            .ignore_then(wire_json_schema_ref())
            .map(|name| DropModel {
                kind: ModelKind::WireJsonSchema,
                name: name.into(),
            }),
        kw(Identifier::Wire)
            .ignore_then(kw(Identifier::Cbor))
            .ignore_then(kw(Identifier::Schema))
            .ignore_then(wire_cbor_schema_ref())
            .map(|name| DropModel {
                kind: ModelKind::WireCborSchema,
                name: name.into(),
            }),
        kw(Identifier::Wire)
            .ignore_then(kw(Identifier::Avro))
            .ignore_then(kw(Identifier::Schema))
            .ignore_then(wire_avro_schema_ref())
            .map(|name| DropModel {
                kind: ModelKind::WireAvroSchema,
                name: name.into(),
            }),
        kw(Identifier::Codec)
            .ignore_then(codec_ref())
            .map(|name| DropModel {
                kind: ModelKind::Codec,
                name: name.into(),
            }),
        kw(Identifier::Client)
            .ignore_then(client_ref())
            .map(|name| DropModel {
                kind: ModelKind::Client,
                name: name.into(),
            }),
        kw(Identifier::Vhost)
            .ignore_then(vhost_ref())
            .map(|name| DropModel {
                kind: ModelKind::Vhost,
                name: name.into(),
            }),
        kw(Identifier::Endpoint)
            .ignore_then(endpoint_ref())
            .map(|name| DropModel {
                kind: ModelKind::Endpoint,
                name: name.into(),
            }),
        kw(Identifier::Ingestor)
            .ignore_then(ingestor_ref())
            .map(|name| DropModel {
                kind: ModelKind::Ingestor,
                name: name.into(),
            }),
        kw(Identifier::Reingestor)
            .ignore_then(reingestor_ref())
            .map(|name| DropModel {
                kind: ModelKind::Reingestor,
                name: name.into(),
            }),
        kw(Identifier::Reorderer)
            .ignore_then(reorderer_ref())
            .map(|name| DropModel {
                kind: ModelKind::Reorderer,
                name: name.into(),
            }),
        kw(Identifier::Inferencer)
            .ignore_then(inferencer_ref())
            .map(|name| DropModel {
                kind: ModelKind::Inferencer,
                name: name.into(),
            }),
        kw(Identifier::Relay)
            .ignore_then(relay_ref())
            .map(|name| DropModel {
                kind: ModelKind::Relay,
                name: name.into(),
            }),
        kw(Identifier::Junction)
            .ignore_then(junction_ref())
            .map(|name| DropModel {
                kind: ModelKind::Junction,
                name: name.into(),
            }),
        kw(Identifier::Deduplicator)
            .ignore_then(deduplicator_ref())
            .map(|name| DropModel {
                kind: ModelKind::Deduplicator,
                name: name.into(),
            }),
        kw(Identifier::Correlator)
            .ignore_then(correlator_ref())
            .map(|name| DropModel {
                kind: ModelKind::Correlator,
                name: name.into(),
            }),
        kw(Identifier::Emitter)
            .ignore_then(emitter_ref())
            .map(|name| DropModel {
                kind: ModelKind::Emitter,
                name: name.into(),
            }),
        kw(Identifier::Placement)
            .ignore_then(placement_ref())
            .map(|name| DropModel {
                kind: ModelKind::Placement,
                name: name.into(),
            }),
        kw(Identifier::Udf)
            .ignore_then(udf_ref())
            .map(|name| DropModel {
                kind: ModelKind::Udf,
                name: name.into(),
            }),
    );

    kw(Identifier::Drop)
        .ignore_then(target)
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

pub fn parse_drop(input: &str) -> Result<DropModel, ParseFromSourceError> {
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
    use nervix_models::ClusterNodeName;

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
        assert!(suggestions.contains(&"RELAY".to_string()));
        assert!(suggestions.contains(&"JUNCTION".to_string()));
        assert!(suggestions.contains(&"DEDUPLICATOR".to_string()));
        assert!(suggestions.contains(&"EMITTER".to_string()));
        assert!(suggestions.contains(&"PLACEMENT".to_string()));
        assert!(suggestions.contains(&"UDF".to_string()));
    }
}
