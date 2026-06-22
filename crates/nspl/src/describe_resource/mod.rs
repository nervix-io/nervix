use chumsky::prelude::*;
use nervix_models::DescribeResource;

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, ParseFromSourceError, current_word_prefix, into_parse_error, kw, lex_input,
        resource_ref, suggestions_from_errors,
    },
};

pub fn describe_resource_parser<'src>()
-> impl Parser<'src, &'src [Token], DescribeResource, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Describe)
        .ignore_then(kw(Identifier::Resource))
        .ignore_then(resource_ref())
        .then(
            kw(Identifier::Version)
                .ignore_then(select! { Token::NumberLiteral(value) => value })
                .try_map(|version, span| {
                    version.parse::<u64>().map_or_else(
                        |_| {
                            Err(Rich::custom(
                                span,
                                "resource_version must be an unsigned integer",
                            ))
                        },
                        Ok,
                    )
                })
                .or_not(),
        )
        .map(|(identifier, version)| DescribeResource {
            identifier,
            version,
        })
        .then_ignore(crate::parser_support::tok(Token::Semicolon).or_not())
}

pub fn parse_describe_resource_tokens(
    tokens: &[Token],
) -> Result<DescribeResource, Vec<ParseError<'_>>> {
    let out = describe_resource_parser().then_ignore(end()).parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .expect("successful parse must have output"))
    }
}

pub fn parse_describe_resource(input: &str) -> Result<DescribeResource, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_describe_resource_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_describe_resource(input: &str, cursor: usize) -> Vec<String> {
    let safe_cursor = cursor.min(input.len());
    let prefix_src = &input[..safe_cursor];
    let prefix = current_word_prefix(prefix_src);

    let (_, _, tokens) = match lex_input(prefix_src) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    let out = describe_resource_parser()
        .then_ignore(end())
        .parse(tokens.as_slice());
    if !out.has_errors() {
        let trimmed = prefix_src.trim_end();
        if prefix_src.len() > trimmed.len()
            && trimmed
                .to_ascii_uppercase()
                .starts_with("DESCRIBE RESOURCE ")
            && !trimmed.to_ascii_uppercase().contains(" VERSION ")
        {
            return vec!["VERSION".to_string()];
        }
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
    fn parses_describe_resource() {
        let tokens = to_tokens("DESCRIBE RESOURCE fraud_model VERSION 7;");
        let parsed = parse_describe_resource_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.identifier.as_str(), "fraud_model");
        assert_eq!(parsed.version, Some(7));
    }

    #[test]
    fn parses_describe_resource_summary() {
        let tokens = to_tokens("DESCRIBE RESOURCE fraud_model;");
        let parsed = parse_describe_resource_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.identifier.as_str(), "fraud_model");
        assert_eq!(parsed.version, None);
    }

    #[test]
    fn rejects_non_numeric_version() {
        let err = parse_describe_resource("DESCRIBE RESOURCE fraud_model VERSION abc;")
            .expect_err("parse should fail");
        match err {
            ParseFromSourceError::Parse { diagnostics, .. } => {
                assert!(!diagnostics.is_empty());
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn suggests_resource_keyword_after_describe() {
        let input = "DESCRIBE ";
        let suggestions = suggest_describe_resource(input, input.len());
        assert!(suggestions.contains(&"RESOURCE".to_string()));
    }

    #[test]
    fn suggests_version_keyword_after_identifier() {
        let input = "DESCRIBE RESOURCE fraud_model ";
        let suggestions = suggest_describe_resource(input, input.len());
        assert!(suggestions.contains(&"VERSION".to_string()));
    }
}
