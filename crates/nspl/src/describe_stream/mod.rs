use chumsky::prelude::*;
use nervix_models::DescribeRelay;

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, ParseFromSourceError, current_word_prefix, into_parse_error, kw, lex_input,
        relay_ref, suggestions_from_errors,
    },
    subscribe::subscription_bindings_parser,
};

pub fn describe_stream_parser<'src>()
-> impl Parser<'src, &'src [Token], DescribeRelay, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Describe)
        .ignore_then(kw(Identifier::Relay))
        .ignore_then(relay_ref())
        .then(subscription_bindings_parser().or_not())
        .map(|(relay, bindings)| DescribeRelay {
            relay,
            bindings: bindings.unwrap_or_default(),
        })
        .then_ignore(crate::parser_support::tok(Token::Semicolon).or_not())
}

pub fn parse_describe_stream_tokens(
    tokens: &[Token],
) -> Result<DescribeRelay, Vec<ParseError<'_>>> {
    let out = describe_stream_parser().then_ignore(end()).parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .expect("successful parse must have output"))
    }
}

pub fn parse_describe_stream(input: &str) -> Result<DescribeRelay, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_describe_stream_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_describe_stream(input: &str, cursor: usize) -> Vec<String> {
    let safe_cursor = cursor.min(input.len());
    let prefix_src = &input[..safe_cursor];
    let prefix = current_word_prefix(prefix_src);

    let (_, _, tokens) = match lex_input(prefix_src) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    let out = describe_stream_parser()
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
    fn parses_describe_stream() {
        let tokens =
            to_tokens("DESCRIBE RELAY notifications WHERE (tenant = 'acme', user_id = 42);");
        let parsed = parse_describe_stream_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.relay.as_str(), "notifications");
        assert_eq!(parsed.bindings.len(), 2);
        assert_eq!(parsed.bindings[0].field.as_str(), "tenant");
        assert_eq!(
            parsed.bindings[0].value,
            SubscriptionLiteral::String("acme".to_string())
        );
        assert_eq!(parsed.bindings[1].field.as_str(), "user_id");
        assert_eq!(
            parsed.bindings[1].value,
            SubscriptionLiteral::Number("42".to_string())
        );
    }

    #[test]
    fn parses_describe_stream_without_bindings() {
        let tokens = to_tokens("DESCRIBE RELAY notifications;");
        let parsed = parse_describe_stream_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.relay.as_str(), "notifications");
        assert!(parsed.bindings.is_empty());
    }

    #[test]
    fn rejects_describe_stream_from_form() {
        let tokens = to_tokens("DESCRIBE RELAY FROM notifications WHERE (tenant = 'acme');");
        assert!(parse_describe_stream_tokens(&tokens).is_err());
    }

    #[test]
    fn suggests_stream_keyword_after_describe() {
        let input = "DESCRIBE ";
        let suggestions = suggest_describe_stream(input, input.len());
        assert!(suggestions.contains(&"RELAY".to_string()));
    }

    #[test]
    fn suggests_field_name_after_describe_where_paren() {
        let input = "DESCRIBE RELAY notifications WHERE (";
        let suggestions = suggest_describe_stream(input, input.len());
        assert!(suggestions.contains(&"field_name".to_string()));
    }
}
