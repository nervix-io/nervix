use chumsky::prelude::*;
use nervix_models::DescribeDeduplicator;

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, ParseFromSourceError, deduplicator_ref, into_parse_error, kw, lex_input,
        suggest_from, tok,
    },
};

pub fn describe_deduplicator_parser<'src>()
-> impl Parser<'src, &'src [Token], DescribeDeduplicator, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Describe)
        .ignore_then(kw(Identifier::Deduplicator))
        .ignore_then(deduplicator_ref())
        .map(|name| DescribeDeduplicator { name })
        .then_ignore(tok(Token::Semicolon).or_not())
}

pub fn parse_describe_deduplicator_tokens(
    tokens: &[Token],
) -> Result<DescribeDeduplicator, Vec<ParseError<'_>>> {
    let out = describe_deduplicator_parser()
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

pub fn parse_describe_deduplicator(
    input: &str,
) -> Result<DescribeDeduplicator, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_describe_deduplicator_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_describe_deduplicator(input: &str, cursor: usize) -> Vec<String> {
    suggest_from!(input, cursor, describe_deduplicator_parser())
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
    fn parses_describe_deduplicator() {
        let tokens = to_tokens("DESCRIBE DEDUPLICATOR dedup_txns;");
        let parsed = parse_describe_deduplicator_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.name.as_str(), "dedup_txns");
    }

    #[test]
    fn suggests_deduplicator_reference_after_describe_deduplicator() {
        let input = "DESCRIBE DEDUPLICATOR ";
        let suggestions = suggest_describe_deduplicator(input, input.len());
        assert!(suggestions.contains(&"ref:deduplicator".to_string()));
    }
}
