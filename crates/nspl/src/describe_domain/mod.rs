use chumsky::prelude::*;
use nervix_models::DescribeDomain;

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, ParseFromSourceError, current_word_prefix, into_parse_error, kw, lex_input,
        suggestions_from_errors, tok,
    },
};

pub fn describe_domain_parser<'src>()
-> impl Parser<'src, &'src [Token], DescribeDomain, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Describe)
        .ignore_then(kw(Identifier::Domain))
        .to(DescribeDomain)
        .then_ignore(tok(Token::Semicolon).or_not())
}

pub fn parse_describe_domain_tokens(
    tokens: &[Token],
) -> Result<DescribeDomain, Vec<ParseError<'_>>> {
    let out = describe_domain_parser().then_ignore(end()).parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .expect("successful parse must have output"))
    }
}

pub fn parse_describe_domain(input: &str) -> Result<DescribeDomain, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_describe_domain_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_describe_domain(input: &str, cursor: usize) -> Vec<String> {
    let safe_cursor = cursor.min(input.len());
    let prefix_src = &input[..safe_cursor];
    let prefix = current_word_prefix(prefix_src);

    let (_, _, tokens) = match lex_input(prefix_src) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    let out = describe_domain_parser()
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
    fn parses_describe_domain() {
        let tokens = to_tokens("DESCRIBE DOMAIN;");
        parse_describe_domain_tokens(&tokens).expect("parse should succeed");
    }

    #[test]
    fn rejects_named_domain_form() {
        let tokens = to_tokens("DESCRIBE DOMAIN prod;");
        assert!(parse_describe_domain_tokens(&tokens).is_err());
    }

    #[test]
    fn suggests_domain_keyword_after_describe() {
        let input = "DESCRIBE ";
        let suggestions = suggest_describe_domain(input, input.len());
        assert!(suggestions.contains(&"DOMAIN".to_string()));
    }
}
