//! `DESCRIBE PARAMETRIZER <branch>`: a branch's declaration and its partition epoch.

use chumsky::prelude::*;
use meticulous::OptionExt as _;
use nervix_models::DescribeParametrizer;

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        LexedInput, ParseError, ParseFromSourceError, into_parse_error, kw, lex_input,
        parametrizer_ref, suggest_from, tok,
    },
};

pub fn describe_parametrizer_parser<'src>()
-> impl Parser<'src, &'src [Token], DescribeParametrizer, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Describe)
        .ignore_then(kw(Identifier::Parametrizer))
        .ignore_then(parametrizer_ref())
        .map(|parametrizer| DescribeParametrizer { parametrizer })
        .then_ignore(tok(Token::Semicolon).or_not())
}

pub fn parse_describe_parametrizer_tokens(
    tokens: &[Token],
) -> Result<DescribeParametrizer, Vec<ParseError<'_>>> {
    let out = describe_parametrizer_parser()
        .then_ignore(end())
        .parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .verified("has_errors returned false above, so this parse produced output"))
    }
}

pub fn parse_describe_parametrizer(
    input: &str,
) -> error_stack::Result<DescribeParametrizer, ParseFromSourceError> {
    let LexedInput {
        source,
        spanned_tokens,
        tokens,
    } = lex_input(input)?;
    parse_describe_parametrizer_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_describe_parametrizer(input: &str, cursor: usize) -> Vec<String> {
    suggest_from!(input, cursor, describe_parametrizer_parser())
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
    fn parses_describe_parametrizer() {
        let tokens = to_tokens("DESCRIBE PARAMETRIZER by_tenant;");
        let parsed = parse_describe_parametrizer_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.parametrizer.as_str(), "by_tenant");
    }

    #[test]
    fn parses_describe_parametrizer_in_any_keyword_case_without_a_semicolon() {
        let parsed = parse_describe_parametrizer("describe Parametrizer by_tenant")
            .expect("parse should succeed");
        assert_eq!(parsed.parametrizer.as_str(), "by_tenant");
    }

    #[test]
    fn refuses_describe_parametrizer_without_a_parametrizer() {
        let error = parse_describe_parametrizer("DESCRIBE PARAMETRIZER;")
            .expect_err("a parametrizer is required");
        assert!(format!("{error:#}").contains("parametrizer"), "{error:#}");
    }

    #[test]
    fn suggests_a_parametrizer_reference_after_describe_parametrizer() {
        let input = "DESCRIBE PARAMETRIZER ";
        let suggestions = suggest_describe_parametrizer(input, input.len());
        assert!(
            suggestions.contains(&"ref:parametrizer".to_string()),
            "{suggestions:?}"
        );
    }
}
