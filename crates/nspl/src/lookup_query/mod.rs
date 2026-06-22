use chumsky::prelude::*;
use nervix_models::LookupQuery;

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, ParseFromSourceError, current_word_prefix, into_parse_error, kw, lex_input,
        lookup_ref, suggestions_from_errors, tok,
    },
    subscribe::subscription_literal_parser,
};

pub fn lookup_query_parser<'src>()
-> impl Parser<'src, &'src [Token], LookupQuery, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Lookup)
        .ignore_then(lookup_ref())
        .then_ignore(kw(Identifier::Key))
        .then(subscription_literal_parser())
        .map(|(name, key)| LookupQuery { name, key })
        .then_ignore(tok(Token::Semicolon).or_not())
}

pub fn parse_lookup_query_tokens(tokens: &[Token]) -> Result<LookupQuery, Vec<ParseError<'_>>> {
    let out = lookup_query_parser().then_ignore(end()).parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .expect("successful parse must have output"))
    }
}

pub fn parse_lookup_query(input: &str) -> Result<LookupQuery, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_lookup_query_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_lookup_query(input: &str, cursor: usize) -> Vec<String> {
    let safe_cursor = cursor.min(input.len());
    let prefix_src = &input[..safe_cursor];
    let prefix = current_word_prefix(prefix_src);

    let (_, _, tokens) = match lex_input(prefix_src) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    let out = lookup_query_parser()
        .then_ignore(end())
        .parse(tokens.as_slice());
    if !out.has_errors() {
        return Vec::new();
    }

    suggestions_from_errors(out.into_errors(), &prefix)
}

#[cfg(test)]
mod tests {
    use nervix_models::SubscriptionLiteral;

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
    fn parses_lookup_query() {
        let tokens = to_tokens("LOOKUP zip_codes KEY '60601';");
        let parsed = parse_lookup_query_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.name.as_str(), "zip_codes");
        assert_eq!(parsed.key, SubscriptionLiteral::String("60601".to_string()));
    }

    #[test]
    fn suggests_key_after_lookup_reference() {
        let input = "LOOKUP zip_codes ";
        let suggestions = suggest_lookup_query(input, input.len());
        assert!(suggestions.contains(&"KEY".to_string()));
    }
}
