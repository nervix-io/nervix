use chumsky::prelude::*;
use nervix_models::DescribeRouter;

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, ParseFromSourceError, current_word_prefix, into_parse_error, kw, lex_input,
        router_ref, suggestions_from_errors, tok,
    },
};

pub fn describe_router_parser<'src>()
-> impl Parser<'src, &'src [Token], DescribeRouter, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Describe)
        .ignore_then(kw(Identifier::Router))
        .ignore_then(router_ref())
        .map(|name| DescribeRouter { name })
        .then_ignore(tok(Token::Semicolon).or_not())
}

pub fn parse_describe_router_tokens(
    tokens: &[Token],
) -> Result<DescribeRouter, Vec<ParseError<'_>>> {
    let out = describe_router_parser().then_ignore(end()).parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .expect("successful parse must have output"))
    }
}

pub fn parse_describe_router(input: &str) -> Result<DescribeRouter, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_describe_router_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_describe_router(input: &str, cursor: usize) -> Vec<String> {
    let safe_cursor = cursor.min(input.len());
    let prefix_src = &input[..safe_cursor];
    let prefix = current_word_prefix(prefix_src);

    let (_, _, tokens) = match lex_input(prefix_src) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    let out = describe_router_parser()
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
    fn parses_describe_router() {
        let tokens = to_tokens("DESCRIBE ROUTER route_notifications;");
        let parsed = parse_describe_router_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.name.as_str(), "route_notifications");
    }

    #[test]
    fn suggests_router_reference_after_describe_router() {
        let input = "DESCRIBE ROUTER ";
        let suggestions = suggest_describe_router(input, input.len());
        assert!(suggestions.contains(&"ref:router".to_string()));
    }
}
