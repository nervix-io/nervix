//! Plexus endpoint declaration grammar.
//!
//! Layer: language.
//! - **Owns.** Kinded endpoint heads, their body clauses and completion expectations.
//! - **Depends on.** Shared language tokens, parser primitives and vocabulary Models.
//! - **Must not know.** Registry policy, listener routing or signaling execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use chumsky::prelude::*;
use meticulous::OptionExt as _;
use nervix_models::{CreateEndpoint, CreateStatement, EndpointKind, EndpointName, VhostName};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        LexedInput, ParseError, ParseFromSourceError, endpoint_name, if_not_exists_clause,
        into_parse_error, kw, kw_phrase2, lex_input, signaling_protocol_clause, string_lit,
        suggest_from, vhost_ref,
    },
};

#[derive(Clone)]
struct EndpointProperties {
    name: EndpointName,
    on_vhost: VhostName,
    path: String,
}

impl EndpointProperties {
    fn into_endpoint(self, kind: EndpointKind) -> CreateEndpoint {
        CreateEndpoint {
            labels: Default::default(),
            name: self.name,
            on_vhost: self.on_vhost,
            path: self.path,
            kind,
        }
    }
}

fn endpoint_properties<'src>()
-> impl Parser<'src, &'src [Token], EndpointProperties, extra::Err<ParseError<'src>>> + Clone {
    let path = string_lit()
        .labelled("endpoint_path")
        .try_map(|path, span| {
            if path.starts_with('/') {
                Ok(path)
            } else {
                Err(Rich::custom(
                    span,
                    format!("invalid endpoint path '{path}': must start with '/'"),
                ))
            }
        });
    endpoint_name()
        .then_ignore(kw(Identifier::On))
        .then(vhost_ref())
        .then_ignore(kw(Identifier::Path))
        .then(path)
        .map(|((name, on_vhost), path)| EndpointProperties {
            name,
            on_vhost,
            path,
        })
}

pub fn create_endpoint_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateEndpoint>, extra::Err<ParseError<'src>>>
+ Clone {
    crate::labels::finish_creation({
        let properties = endpoint_properties();
        let http = kw_phrase2(Identifier::Http, Identifier::Endpoint)
            .ignore_then(properties.clone())
            .map(|properties| properties.into_endpoint(EndpointKind::Http));
        let websockets = kw_phrase2(Identifier::Websockets, Identifier::Endpoint)
            .ignore_then(properties)
            .then(signaling_protocol_clause().or_not())
            .map(|(properties, signaling_protocol)| {
                properties.into_endpoint(EndpointKind::Websockets { signaling_protocol })
            });
        kw(Identifier::Create)
            .ignore_then(if_not_exists_clause())
            .then(choice((http, websockets)))
            .map(|(if_not_exists, endpoint)| CreateStatement::new(endpoint, if_not_exists))
    })
}

pub fn parse_create_endpoint_tokens(
    tokens: &[Token],
) -> Result<CreateStatement<CreateEndpoint>, Vec<ParseError<'_>>> {
    let out = create_endpoint_parser().then_ignore(end()).parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .verified("has_errors returned false above, so this parse produced output"))
    }
}

pub fn parse_create_endpoint(
    input: &str,
) -> error_stack::Result<CreateStatement<CreateEndpoint>, ParseFromSourceError> {
    let LexedInput {
        source,
        spanned_tokens,
        tokens,
    } = lex_input(input)?;
    parse_create_endpoint_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_create_endpoint(input: &str, cursor: usize) -> Vec<String> {
    suggest_from!(input, cursor, create_endpoint_parser())
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;
    use nervix_models::EndpointType;

    use super::*;
    use crate::lexer::lex;

    fn to_tokens(input: &str) -> Vec<Token> {
        lex(input)
            .assured("lexer should succeed")
            .into_iter()
            .map(|t| t.token)
            .collect()
    }

    #[test]
    fn parses_create_endpoint() {
        let tokens = to_tokens("CREATE WEBSOCKETS ENDPOINT my_ws ON edge PATH '/ws';");
        let parsed = parse_create_endpoint_tokens(&tokens).assured("parse should succeed");
        assert_eq!(parsed.name.as_str(), "my_ws");
        assert_eq!(parsed.on_vhost.as_str(), "edge");
        assert_eq!(parsed.path, "/ws");
        assert_eq!(parsed.kind.endpoint_type(), EndpointType::Websockets);
        assert_eq!(parsed.kind.signaling_protocol(), None);
    }

    #[test]
    fn parses_create_http_endpoint() {
        let tokens = to_tokens("CREATE HTTP ENDPOINT my_http ON edge PATH '/ingest';");
        let parsed = parse_create_endpoint_tokens(&tokens).assured("parse should succeed");
        assert_eq!(parsed.name.as_str(), "my_http");
        assert_eq!(parsed.on_vhost.as_str(), "edge");
        assert_eq!(parsed.path, "/ingest");
        assert_eq!(parsed.kind.endpoint_type(), EndpointType::Http);
        assert_eq!(parsed.kind.signaling_protocol(), None);
    }

    #[test]
    fn parses_create_websocket_endpoint_with_signaling_protocol() {
        let tokens = to_tokens(
            "CREATE WEBSOCKETS ENDPOINT my_ws ON edge PATH '/ws' WITH SIGNALING PROTOCOL \
             binance_style;",
        );
        let parsed = parse_create_endpoint_tokens(&tokens).assured("parse should succeed");

        assert_eq!(
            parsed
                .kind
                .signaling_protocol()
                .map(nervix_models::SignalingProtocolName::as_str),
            Some("binance_style")
        );
    }

    #[test]
    fn rejects_http_endpoint_with_signaling_protocol() {
        let tokens = to_tokens(
            "CREATE HTTP ENDPOINT my_http ON edge PATH '/ingest' WITH SIGNALING PROTOCOL \
             binance_style;",
        );

        assert!(parse_create_endpoint_tokens(&tokens).is_err());
    }

    #[test]
    fn rejects_path_without_leading_slash() {
        let tokens = to_tokens("CREATE WEBSOCKETS ENDPOINT my_ws ON edge PATH 'ws';");
        assert!(parse_create_endpoint_tokens(&tokens).is_err());
    }

    #[test]
    fn suggests_endpoint_name_after_keyword() {
        let suggestions =
            suggest_create_endpoint("CREATE HTTP ENDPOINT ", "CREATE HTTP ENDPOINT ".len());
        assert!(suggestions.contains(&"endpoint_name".to_string()));
    }

    #[test]
    fn kinded_endpoint_heads_select_the_body_grammar() {
        for source in [
            "CREATE HTTP ENDPOINT intake ON edge PATH '/events';",
            "create if not exists http endpoint intake on edge path '/events';",
            "CREATE WEBSOCKETS ENDPOINT intake ON edge PATH '/events';",
            "CREATE IF NOT EXISTS WEBSOCKETS ENDPOINT intake ON edge PATH '/events' WITH \
             SIGNALING PROTOCOL handshake;",
        ] {
            parse_create_endpoint(source).assured("the kinded endpoint head selects its body");
        }
        for source in [
            "CREATE HTTP ENDPOINT intake ON edge PATH '/events' WITH SIGNALING PROTOCOL handshake;",
            "CREATE HTTP ENDPOINT intake FROM upstream;",
            "CREATE WEBSOCKETS ENDPOINT intake ON edge PATH '/events' MODE NO ACK;",
        ] {
            assert!(parse_create_endpoint(source).is_err(), "{source}");
        }
    }

    #[test]
    fn endpoint_head_completion_keeps_ingestor_and_endpoint_bodies_separate() {
        use crate::client_statement::suggest_client_statement;

        let start = "CREATE ";
        let suggestions = suggest_client_statement(start, start.len());
        assert!(
            suggestions.contains(&"HTTP ENDPOINT".to_string()),
            "{suggestions:?}"
        );
        assert!(
            suggestions.contains(&"WEBSOCKETS ENDPOINT".to_string()),
            "{suggestions:?}"
        );
        for kind in ["HTTP", "WEBSOCKETS"] {
            let head = format!("CREATE {kind} ENDPOINT ");
            let suggestions = suggest_client_statement(&head, head.len());
            assert!(
                suggestions.contains(&"endpoint_name".to_string()),
                "{suggestions:?}"
            );
            assert!(
                suggestions.contains(&"INGESTOR".to_string()),
                "{suggestions:?}"
            );
            let body = format!("CREATE {kind} ENDPOINT intake ");
            assert_eq!(suggest_client_statement(&body, body.len()), vec!["ON"]);
            let source = format!("CREATE {kind} ENDPOINT INGESTOR intake ");
            assert_eq!(
                suggest_client_statement(&source, source.len()),
                vec!["FROM"]
            );
        }
        let http = "CREATE HTTP ENDPOINT intake ON edge PATH '/events' ";
        assert_eq!(suggest_client_statement(http, http.len()), [";", "LABELS"]);
        let websocket = concat!(
            "CREATE WEBSOCKETS ENDPOINT intake ON edge PATH '/events' ",
            "WITH SIGNALING PROTOCOL "
        );
        assert_eq!(
            suggest_client_statement(websocket, websocket.len()),
            vec!["ref:signaling_protocol"]
        );
    }
}
