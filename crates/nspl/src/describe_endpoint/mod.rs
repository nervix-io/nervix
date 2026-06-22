use chumsky::prelude::*;
use nervix_models::DescribeEndpoint;

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, ParseFromSourceError, current_word_prefix, endpoint_ref, into_parse_error, kw,
        lex_input, suggestions_from_errors,
    },
};

pub fn describe_endpoint_parser<'src>()
-> impl Parser<'src, &'src [Token], DescribeEndpoint, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Describe)
        .ignore_then(kw(Identifier::Endpoint))
        .ignore_then(endpoint_ref())
        .map(|name| DescribeEndpoint { name })
        .then_ignore(crate::parser_support::tok(Token::Semicolon).or_not())
}

pub fn parse_describe_endpoint_tokens(
    tokens: &[Token],
) -> Result<DescribeEndpoint, Vec<ParseError<'_>>> {
    let out = describe_endpoint_parser().then_ignore(end()).parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .expect("successful parse must have output"))
    }
}

pub fn parse_describe_endpoint(input: &str) -> Result<DescribeEndpoint, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_describe_endpoint_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_describe_endpoint(input: &str, cursor: usize) -> Vec<String> {
    let safe_cursor = cursor.min(input.len());
    let prefix_src = &input[..safe_cursor];
    let prefix = current_word_prefix(prefix_src);

    let (_, _, tokens) = match lex_input(prefix_src) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    let out = describe_endpoint_parser()
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
    fn parses_describe_endpoint() {
        let tokens = to_tokens("DESCRIBE ENDPOINT http_notifications_endpoint;");
        let parsed = parse_describe_endpoint_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.name.as_str(), "http_notifications_endpoint");
    }

    #[test]
    fn rejects_describe_endpoint_from_form() {
        let tokens = to_tokens("DESCRIBE ENDPOINT FROM http_notifications_endpoint;");
        assert!(parse_describe_endpoint_tokens(&tokens).is_err());
    }

    #[test]
    fn suggests_endpoint_reference_after_describe_endpoint() {
        let input = "DESCRIBE ENDPOINT ";
        let suggestions = suggest_describe_endpoint(input, input.len());
        assert!(suggestions.contains(&"ref:endpoint".to_string()));
    }
}
