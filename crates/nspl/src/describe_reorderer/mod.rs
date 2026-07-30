use chumsky::prelude::*;
use nervix_models::DescribeReorderer;

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, ParseFromSourceError, into_parse_error, kw, lex_input, reorderer_ref,
        suggest_from, tok,
    },
};

pub fn describe_reorderer_parser<'src>()
-> impl Parser<'src, &'src [Token], DescribeReorderer, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Describe)
        .ignore_then(kw(Identifier::Reorderer))
        .ignore_then(reorderer_ref())
        .map(|name| DescribeReorderer { name })
        .then_ignore(tok(Token::Semicolon).or_not())
}

pub fn parse_describe_reorderer_tokens(
    tokens: &[Token],
) -> Result<DescribeReorderer, Vec<ParseError<'_>>> {
    let out = describe_reorderer_parser().then_ignore(end()).parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .expect("successful parse must have output"))
    }
}

pub fn parse_describe_reorderer(input: &str) -> Result<DescribeReorderer, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_describe_reorderer_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_describe_reorderer(input: &str, cursor: usize) -> Vec<String> {
    suggest_from!(input, cursor, describe_reorderer_parser())
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
    fn parses_describe_reorderer() {
        let tokens = to_tokens("DESCRIBE REORDERER order_notifications;");
        let parsed = parse_describe_reorderer_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.name.as_str(), "order_notifications");
    }

    #[test]
    fn suggests_reorderer_reference_after_describe_reorderer() {
        let input = "DESCRIBE REORDERER ";
        let suggestions = suggest_describe_reorderer(input, input.len());
        assert!(suggestions.contains(&"ref:reorderer".to_string()));
    }
}
