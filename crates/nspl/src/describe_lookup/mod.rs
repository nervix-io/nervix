use chumsky::prelude::*;
use nervix_models::DescribeLookup;

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, ParseFromSourceError, current_word_prefix, into_parse_error, kw, kw_phrase2,
        lex_input, lookup_ref, suggestions_from_errors, tok,
    },
};

pub fn describe_lookup_parser<'src>()
-> impl Parser<'src, &'src [Token], DescribeLookup, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Describe)
        .ignore_then(kw_phrase2(Identifier::Hash, Identifier::Map))
        .ignore_then(lookup_ref())
        .map(|name| DescribeLookup { name })
        .then_ignore(tok(Token::Semicolon).or_not())
}

pub fn parse_describe_lookup_tokens(
    tokens: &[Token],
) -> Result<DescribeLookup, Vec<ParseError<'_>>> {
    let out = describe_lookup_parser().then_ignore(end()).parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .expect("successful parse must have output"))
    }
}

pub fn parse_describe_lookup(input: &str) -> Result<DescribeLookup, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_describe_lookup_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_describe_lookup(input: &str, cursor: usize) -> Vec<String> {
    let safe_cursor = cursor.min(input.len());
    let prefix_src = &input[..safe_cursor];
    let prefix = current_word_prefix(prefix_src);

    let (_, _, tokens) = match lex_input(prefix_src) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    let out = describe_lookup_parser()
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
    fn parses_describe_lookup() {
        let tokens = to_tokens("DESCRIBE HASH MAP zip_codes;");
        let parsed = parse_describe_lookup_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.name.as_str(), "zip_codes");
    }

    #[test]
    fn suggests_lookup_reference_after_describe_lookup() {
        let input = "DESCRIBE HASH MAP ";
        let suggestions = suggest_describe_lookup(input, input.len());
        assert!(suggestions.contains(&"ref:lookup".to_string()));
    }
}
