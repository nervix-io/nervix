use chumsky::prelude::*;
use nervix_models::DescribeCorrelator;

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, ParseFromSourceError, correlator_ref, into_parse_error, kw, lex_input,
        suggest_from, tok,
    },
};

pub fn describe_correlator_parser<'src>()
-> impl Parser<'src, &'src [Token], DescribeCorrelator, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Describe)
        .ignore_then(kw(Identifier::Correlator))
        .ignore_then(correlator_ref())
        .map(|name| DescribeCorrelator { name })
        .then_ignore(tok(Token::Semicolon).or_not())
}

pub fn parse_describe_correlator_tokens(
    tokens: &[Token],
) -> Result<DescribeCorrelator, Vec<ParseError<'_>>> {
    let out = describe_correlator_parser()
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

pub fn parse_describe_correlator(input: &str) -> Result<DescribeCorrelator, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_describe_correlator_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_describe_correlator(input: &str, cursor: usize) -> Vec<String> {
    suggest_from!(input, cursor, describe_correlator_parser())
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
    fn parses_describe_correlator() {
        let tokens = to_tokens("DESCRIBE CORRELATOR correlate_profiles;");
        let parsed = parse_describe_correlator_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.name.as_str(), "correlate_profiles");
    }

    #[test]
    fn suggests_correlator_reference_after_describe_correlator() {
        let input = "DESCRIBE CORRELATOR ";
        let suggestions = suggest_describe_correlator(input, input.len());
        assert!(suggestions.contains(&"ref:correlator".to_string()));
    }
}
