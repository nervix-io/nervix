use chumsky::prelude::*;
use nervix_models::{CreateStatement, CreateVhost, VhostTlsResource};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, ParseFromSourceError, current_word_prefix, hostname_lit, if_not_exists_clause,
        into_parse_error, kw, lex_input, resource_ref, suggestions_from_errors, tok, vhost_name,
    },
};

pub fn create_vhost_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateVhost>, extra::Err<ParseError<'src>>> + Clone
{
    kw(Identifier::Create)
        .ignore_then(if_not_exists_clause())
        .then_ignore(kw(Identifier::Vhost))
        .then(vhost_name())
        .then(
            hostname_lit()
                .separated_by(tok(Token::Comma))
                .at_least(1)
                .collect::<Vec<_>>(),
        )
        .then(
            kw(Identifier::With)
                .ignore_then(kw(Identifier::Tls))
                .ignore_then(resource_ref())
                .then(
                    kw(Identifier::Version)
                        .ignore_then(select! { Token::NumberLiteral(value) => value })
                        .try_map(|value, span| {
                            value
                                .parse::<u64>()
                                .map_err(|_| chumsky::error::Rich::custom(span, "resource_version"))
                        })
                        .or_not(),
                )
                .map(|(resource, version)| VhostTlsResource { resource, version })
                .or_not(),
        )
        .then_ignore(tok(Token::Semicolon).or_not())
        .map(|(((if_not_exists, name), hostnames), tls)| {
            CreateStatement::new(
                CreateVhost {
                    name,
                    hostnames,
                    tls,
                },
                if_not_exists,
            )
        })
}

pub fn parse_create_vhost_tokens(
    tokens: &[Token],
) -> Result<CreateStatement<CreateVhost>, Vec<ParseError<'_>>> {
    let out = create_vhost_parser().then_ignore(end()).parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .expect("successful parse must have output"))
    }
}

pub fn parse_create_vhost(
    input: &str,
) -> Result<CreateStatement<CreateVhost>, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_create_vhost_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_create_vhost(input: &str, cursor: usize) -> Vec<String> {
    let safe_cursor = cursor.min(input.len());
    let prefix_src = &input[..safe_cursor];
    let prefix = current_word_prefix(prefix_src);

    let (_, _, tokens) = match lex_input(prefix_src) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    let out = create_vhost_parser()
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
    fn parses_create_vhost() {
        let input = "CREATE VHOST my_vhost api.example.com, foo-bar.localhost;";
        let tokens = to_tokens(input);
        let parsed = parse_create_vhost_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.name.as_str(), "my_vhost");
        assert_eq!(
            parsed.hostnames,
            vec![
                "api.example.com".to_string(),
                "foo-bar.localhost".to_string()
            ]
        );
        assert_eq!(parsed.tls, None);
    }

    #[test]
    fn parses_create_vhost_with_tls_latest() {
        let input = "CREATE VHOST my_vhost api.example.com WITH TLS my_cert;";
        let tokens = to_tokens(input);
        let parsed = parse_create_vhost_tokens(&tokens).expect("parse should succeed");
        assert_eq!(
            parsed.tls,
            Some(VhostTlsResource {
                resource: nervix_models::Identifier::parse("my_cert").expect("valid resource"),
                version: None,
            })
        );
    }

    #[test]
    fn parses_create_vhost_with_tls_explicit_version() {
        let input = "CREATE VHOST my_vhost api.example.com WITH TLS my_cert VERSION 7;";
        let tokens = to_tokens(input);
        let parsed = parse_create_vhost_tokens(&tokens).expect("parse should succeed");
        assert_eq!(
            parsed.tls,
            Some(VhostTlsResource {
                resource: nervix_models::Identifier::parse("my_cert").expect("valid resource"),
                version: Some(7),
            })
        );
    }

    #[test]
    fn rejects_missing_hostnames() {
        let tokens = to_tokens("CREATE VHOST my_vhost;");
        assert!(parse_create_vhost_tokens(&tokens).is_err());
    }

    #[test]
    fn suggests_vhost_name_after_keyword() {
        let suggestions = suggest_create_vhost("CREATE VHOST ", "CREATE VHOST ".len());
        assert!(suggestions.contains(&"vhost_name".to_string()));
    }

    #[test]
    fn suggests_tls_after_with() {
        let input = "CREATE VHOST edge api.example.com WITH ";
        let suggestions = suggest_create_vhost(input, input.len());
        assert!(suggestions.contains(&"TLS".to_string()));
    }
}
