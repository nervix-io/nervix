use chumsky::prelude::*;
use nervix_models::{CreateSignalingProtocol, CreateStatement, SignalingProtocolOnConnect};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, ParseFromSourceError, current_word_prefix, duration_lit, if_not_exists_clause,
        into_parse_error, kw, lex_input, signaling_protocol_name, string_lit,
        suggestions_from_errors, tok,
    },
};

fn body_list<'src>()
-> impl Parser<'src, &'src [Token], Vec<String>, extra::Err<ParseError<'src>>> + Clone {
    string_lit()
        .separated_by(tok(Token::Comma))
        .at_least(1)
        .collect::<Vec<_>>()
}

pub fn create_signaling_protocol_parser<'src>() -> impl Parser<
    'src,
    &'src [Token],
    CreateStatement<CreateSignalingProtocol>,
    extra::Err<ParseError<'src>>,
> + Clone {
    kw(Identifier::Create)
        .ignore_then(if_not_exists_clause())
        .then_ignore(kw(Identifier::Signaling))
        .then_ignore(kw(Identifier::Protocol))
        .then(signaling_protocol_name())
        .then_ignore(kw(Identifier::On))
        .then_ignore(kw(Identifier::Connect))
        .then_ignore(kw(Identifier::Send))
        .then_ignore(kw(Identifier::Body))
        .then(body_list())
        .then_ignore(kw(Identifier::Wait))
        .then_ignore(kw(Identifier::Body))
        .then(body_list())
        .then_ignore(kw(Identifier::Timeout))
        .then(duration_lit().try_map(|timeout, span| {
            humantime::parse_duration(&timeout)
                .map(|_| timeout.clone())
                .map_err(|error| {
                    Rich::custom(span, format!("invalid duration '{timeout}': {error}"))
                })
        }))
        .then_ignore(tok(Token::Semicolon).or_not())
        .map(
            |((((if_not_exists, name), send_bodies), wait_bodies), timeout)| {
                CreateStatement::new(
                    CreateSignalingProtocol {
                        name,
                        on_connect: SignalingProtocolOnConnect {
                            send_bodies,
                            wait_bodies,
                            timeout,
                        },
                    },
                    if_not_exists,
                )
            },
        )
}

pub fn parse_create_signaling_protocol_tokens(
    tokens: &[Token],
) -> Result<CreateStatement<CreateSignalingProtocol>, Vec<ParseError<'_>>> {
    let out = create_signaling_protocol_parser()
        .then_ignore(end())
        .parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .expect("successful parse must have output"))
    }
}

pub fn parse_create_signaling_protocol(
    input: &str,
) -> Result<CreateStatement<CreateSignalingProtocol>, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_create_signaling_protocol_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_create_signaling_protocol(input: &str, cursor: usize) -> Vec<String> {
    let safe_cursor = cursor.min(input.len());
    let prefix_src = &input[..safe_cursor];
    let prefix = current_word_prefix(prefix_src);

    let (_, _, tokens) = match lex_input(prefix_src) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    let out = create_signaling_protocol_parser()
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
    fn parses_create_signaling_protocol() {
        let tokens = to_tokens(
            r#"
            CREATE SIGNALING PROTOCOL binance_style
              ON CONNECT
              SEND BODY '{"method":"SUBSCRIBE","id":1}', '{"method":"SUBSCRIBE","id":2}'
              WAIT BODY '{"id":1,"result":null}', '{"id":2,"result":null}' TIMEOUT 5s;
            "#,
        );
        let parsed = parse_create_signaling_protocol_tokens(&tokens).expect("parse should succeed");

        assert_eq!(parsed.name.as_str(), "binance_style");
        assert_eq!(parsed.on_connect.send_bodies.len(), 2);
        assert_eq!(parsed.on_connect.wait_bodies.len(), 2);
        assert_eq!(parsed.on_connect.timeout, "5s");
    }

    #[test]
    fn rejects_missing_wait_body() {
        let tokens = to_tokens(
            r#"
            CREATE SIGNALING PROTOCOL missing_wait
              ON CONNECT
              SEND BODY '{"method":"SUBSCRIBE","id":1}'
              WAIT BODY TIMEOUT 5s;
            "#,
        );

        assert!(parse_create_signaling_protocol_tokens(&tokens).is_err());
    }

    #[test]
    fn suggests_signaling_protocol_name_after_keyword() {
        let suggestions = suggest_create_signaling_protocol(
            "CREATE SIGNALING PROTOCOL ",
            "CREATE SIGNALING PROTOCOL ".len(),
        );
        assert!(suggestions.contains(&"signaling_protocol_name".to_string()));
    }
}
