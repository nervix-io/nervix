use chumsky::prelude::*;
use nervix_models::DescribeJunction;

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, ParseFromSourceError, into_parse_error, junction_ref, kw, lex_input,
        suggest_from, tok,
    },
};

pub fn describe_junction_parser<'src>()
-> impl Parser<'src, &'src [Token], DescribeJunction, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Describe)
        .ignore_then(kw(Identifier::Junction))
        .ignore_then(junction_ref())
        .map(|name| DescribeJunction { name })
        .then_ignore(tok(Token::Semicolon).or_not())
}

pub fn parse_describe_junction_tokens(
    tokens: &[Token],
) -> Result<DescribeJunction, Vec<ParseError<'_>>> {
    let out = describe_junction_parser().then_ignore(end()).parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .expect("successful parse must have output"))
    }
}

pub fn parse_describe_junction(input: &str) -> Result<DescribeJunction, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_describe_junction_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_describe_junction(input: &str, cursor: usize) -> Vec<String> {
    suggest_from!(input, cursor, describe_junction_parser())
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
    fn parses_describe_junction() {
        let tokens = to_tokens("DESCRIBE JUNCTION route_notifications;");
        let parsed = parse_describe_junction_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.name.as_str(), "route_notifications");
    }

    #[test]
    fn rejects_describe_junction_without_a_reference() {
        let tokens = to_tokens("DESCRIBE JUNCTION;");
        assert!(parse_describe_junction_tokens(&tokens).is_err());
    }

    #[test]
    fn suggests_junction_reference_after_describe_junction() {
        let input = "DESCRIBE JUNCTION ";
        let suggestions = suggest_describe_junction(input, input.len());
        assert!(suggestions.contains(&"ref:junction".to_string()));
    }
}
