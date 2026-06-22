use chumsky::prelude::*;
use nervix_models::{CreateEndpoint, CreateStatement, EndpointType};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, ParseFromSourceError, current_word_prefix, endpoint_name, if_not_exists_clause,
        into_parse_error, kw, lex_input, signaling_protocol_clause, string_lit,
        suggestions_from_errors, tok, vhost_ref,
    },
};

fn endpoint_type_parser<'src>()
-> impl Parser<'src, &'src [Token], EndpointType, extra::Err<ParseError<'src>>> + Clone {
    choice((
        kw(Identifier::Websockets).to(EndpointType::Websockets),
        kw(Identifier::Http).to(EndpointType::Http),
    ))
}

pub fn create_endpoint_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateEndpoint>, extra::Err<ParseError<'src>>>
+ Clone {
    kw(Identifier::Create)
        .ignore_then(if_not_exists_clause())
        .then_ignore(kw(Identifier::Endpoint))
        .then(endpoint_name())
        .then_ignore(kw(Identifier::On))
        .then(vhost_ref())
        .then_ignore(kw(Identifier::Path))
        .then(string_lit().try_map(|path, span| {
            if path.starts_with('/') {
                Ok(path)
            } else {
                Err(Rich::custom(
                    span,
                    format!("invalid endpoint path '{path}': must start with '/'"),
                ))
            }
        }))
        .then_ignore(kw(Identifier::Type))
        .then(endpoint_type_parser())
        .then(signaling_protocol_clause().or_not())
        .try_map(
            |(((((if_not_exists, name), on_vhost), path), endpoint_type), signaling_protocol),
             span| {
                if signaling_protocol.is_some() && endpoint_type != EndpointType::Websockets {
                    return Err(Rich::custom(
                        span,
                        "SIGNALING PROTOCOL is only valid for WEBSOCKETS endpoints",
                    ));
                }

                Ok((
                    (((if_not_exists, name), on_vhost), path),
                    (endpoint_type, signaling_protocol),
                ))
            },
        )
        .then_ignore(tok(Token::Semicolon).or_not())
        .map(
            |((((if_not_exists, name), on_vhost), path), (endpoint_type, signaling_protocol))| {
                CreateStatement::new(
                    CreateEndpoint {
                        name,
                        on_vhost,
                        path,
                        endpoint_type,
                        signaling_protocol,
                    },
                    if_not_exists,
                )
            },
        )
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
            .expect("successful parse must have output"))
    }
}

pub fn parse_create_endpoint(
    input: &str,
) -> Result<CreateStatement<CreateEndpoint>, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_create_endpoint_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_create_endpoint(input: &str, cursor: usize) -> Vec<String> {
    let safe_cursor = cursor.min(input.len());
    let prefix_src = &input[..safe_cursor];
    let prefix = current_word_prefix(prefix_src);

    let (_, _, tokens) = match lex_input(prefix_src) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    let out = create_endpoint_parser()
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
    fn parses_create_endpoint() {
        let tokens = to_tokens("CREATE ENDPOINT my_ws ON edge PATH '/ws' TYPE WEBSOCKETS;");
        let parsed = parse_create_endpoint_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.name.as_str(), "my_ws");
        assert_eq!(parsed.on_vhost.as_str(), "edge");
        assert_eq!(parsed.path, "/ws");
        assert_eq!(parsed.endpoint_type, EndpointType::Websockets);
        assert_eq!(parsed.signaling_protocol, None);
    }

    #[test]
    fn parses_create_http_endpoint() {
        let tokens = to_tokens("CREATE ENDPOINT my_http ON edge PATH '/ingest' TYPE HTTP;");
        let parsed = parse_create_endpoint_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.name.as_str(), "my_http");
        assert_eq!(parsed.on_vhost.as_str(), "edge");
        assert_eq!(parsed.path, "/ingest");
        assert_eq!(parsed.endpoint_type, EndpointType::Http);
        assert_eq!(parsed.signaling_protocol, None);
    }

    #[test]
    fn parses_create_websocket_endpoint_with_signaling_protocol() {
        let tokens = to_tokens(
            "CREATE ENDPOINT my_ws ON edge PATH '/ws' TYPE WEBSOCKETS WITH SIGNALING PROTOCOL \
             binance_style;",
        );
        let parsed = parse_create_endpoint_tokens(&tokens).expect("parse should succeed");

        assert_eq!(
            parsed
                .signaling_protocol
                .as_ref()
                .map(nervix_models::Identifier::as_str),
            Some("binance_style")
        );
    }

    #[test]
    fn rejects_http_endpoint_with_signaling_protocol() {
        let tokens = to_tokens(
            "CREATE ENDPOINT my_http ON edge PATH '/ingest' TYPE HTTP WITH SIGNALING PROTOCOL \
             binance_style;",
        );

        assert!(parse_create_endpoint_tokens(&tokens).is_err());
    }

    #[test]
    fn rejects_path_without_leading_slash() {
        let tokens = to_tokens("CREATE ENDPOINT my_ws ON edge PATH 'ws' TYPE WEBSOCKETS;");
        assert!(parse_create_endpoint_tokens(&tokens).is_err());
    }

    #[test]
    fn suggests_endpoint_name_after_keyword() {
        let suggestions = suggest_create_endpoint("CREATE ENDPOINT ", "CREATE ENDPOINT ".len());
        assert!(suggestions.contains(&"endpoint_name".to_string()));
    }
}
