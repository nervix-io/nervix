use chumsky::prelude::*;
use nervix_models::DescribeIngestor;

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, ParseFromSourceError, current_word_prefix, ingestor_ref, into_parse_error, kw,
        lex_input, suggestions_from_errors,
    },
};

pub fn describe_ingestor_parser<'src>()
-> impl Parser<'src, &'src [Token], DescribeIngestor, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Describe)
        .ignore_then(kw(Identifier::Ingestor))
        .ignore_then(ingestor_ref())
        .map(|ingestor| DescribeIngestor { ingestor })
        .then_ignore(crate::parser_support::tok(Token::Semicolon).or_not())
}

pub fn parse_describe_ingestor_tokens(
    tokens: &[Token],
) -> Result<DescribeIngestor, Vec<ParseError<'_>>> {
    let out = describe_ingestor_parser().then_ignore(end()).parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .expect("successful parse must have output"))
    }
}

pub fn parse_describe_ingestor(input: &str) -> Result<DescribeIngestor, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_describe_ingestor_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_describe_ingestor(input: &str, cursor: usize) -> Vec<String> {
    let safe_cursor = cursor.min(input.len());
    let prefix_src = &input[..safe_cursor];
    let prefix = current_word_prefix(prefix_src);

    let (_, _, tokens) = match lex_input(prefix_src) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    let out = describe_ingestor_parser()
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
    fn parses_describe_ingestor() {
        let tokens = to_tokens("DESCRIBE INGESTOR kafka_notifications;");
        let parsed = parse_describe_ingestor_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.ingestor.as_str(), "kafka_notifications");
    }

    #[test]
    fn rejects_describe_ingestor_from_form() {
        let tokens = to_tokens("DESCRIBE INGESTOR FROM kafka_notifications;");
        assert!(parse_describe_ingestor_tokens(&tokens).is_err());
    }

    #[test]
    fn rejects_missing_ingestor_reference() {
        let tokens = to_tokens("DESCRIBE INGESTOR ;");
        assert!(parse_describe_ingestor_tokens(&tokens).is_err());
    }

    #[test]
    fn suggests_ingestor_keyword_after_describe() {
        let input = "DESCRIBE ";
        let suggestions = suggest_describe_ingestor(input, input.len());
        assert!(suggestions.contains(&"INGESTOR".to_string()));
    }

    #[test]
    fn suggests_ingestor_reference_after_describe_ingestor() {
        let input = "DESCRIBE INGESTOR ";
        let suggestions = suggest_describe_ingestor(input, input.len());
        assert!(suggestions.contains(&"ref:ingestor".to_string()));
    }
}
