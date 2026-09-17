use chumsky::prelude::*;
use meticulous::OptionExt as _;
use nervix_models::{CreateStatement, CreateVhost, RequestedResourceVersion, VhostTlsResource};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        LexedInput, ParseError, ParseFromSourceError, hostname_lit, if_not_exists_clause,
        into_parse_error, kw, lex_input, resource_ref, resource_version_clause, suggest_from, tok,
        vhost_name,
    },
};

pub fn create_vhost_parser<'src>() -> impl Parser<
    'src,
    &'src [Token],
    CreateStatement<CreateVhost<RequestedResourceVersion>>,
    extra::Err<ParseError<'src>>,
> + Clone {
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
                .then(resource_version_clause())
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
) -> Result<CreateStatement<CreateVhost<RequestedResourceVersion>>, Vec<ParseError<'_>>> {
    let out = create_vhost_parser().then_ignore(end()).parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .verified("has_errors returned false above, so this parse produced output"))
    }
}

pub fn parse_create_vhost(
    input: &str,
) -> Result<CreateStatement<CreateVhost<RequestedResourceVersion>>, ParseFromSourceError> {
    let LexedInput {
        source,
        spanned_tokens,
        tokens,
    } = lex_input(input)?;
    parse_create_vhost_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_create_vhost(input: &str, cursor: usize) -> Vec<String> {
    suggest_from!(input, cursor, create_vhost_parser())
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
    fn parses_create_vhost_with_tls_latest_version() {
        let input = "CREATE VHOST my_vhost api.example.com WITH TLS my_cert VERSION LATEST;";
        let tokens = to_tokens(input);
        let parsed = parse_create_vhost_tokens(&tokens).expect("parse should succeed");
        assert_eq!(
            parsed.tls,
            Some(VhostTlsResource {
                resource: nervix_models::ResourceName::parse("my_cert").expect("valid resource"),
                version: RequestedResourceVersion::Latest,
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
                resource: nervix_models::ResourceName::parse("my_cert").expect("valid resource"),
                version: RequestedResourceVersion::Number(7),
            })
        );
    }

    #[test]
    fn requires_a_version_for_the_tls_resource() {
        for input in [
            "CREATE VHOST my_vhost api.example.com WITH TLS my_cert;",
            "CREATE VHOST my_vhost api.example.com WITH TLS my_cert VERSION;",
            "CREATE VHOST my_vhost api.example.com WITH TLS my_cert VERSION newest;",
        ] {
            assert!(
                parse_create_vhost_tokens(&to_tokens(input)).is_err(),
                "{input:?} must be rejected"
            );
        }
    }

    #[test]
    fn suggests_only_version_after_the_tls_resource() {
        let input = "CREATE VHOST edge api.example.com WITH TLS my_cert ";
        let suggestions = suggest_create_vhost(input, input.len());
        assert_eq!(suggestions, vec!["VERSION".to_string()]);
    }

    #[test]
    fn suggests_latest_and_a_completed_version_after_version() {
        let input = "CREATE VHOST edge api.example.com WITH TLS my_cert VERSION ";
        let suggestions = suggest_create_vhost(input, input.len());
        assert!(suggestions.contains(&"LATEST".to_string()));
        assert!(suggestions.contains(&"completed_resource_version".to_string()));
        assert!(!suggestions.contains(&"integer_literal".to_string()));
        assert!(!suggestions.contains(&";".to_string()));
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
