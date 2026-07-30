use chumsky::prelude::*;
use nervix_models::DescribeWindowProcessor;

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, ParseFromSourceError, into_parse_error, kw, lex_input, suggest_from, tok,
        window_processor_ref,
    },
};

pub fn describe_window_processor_parser<'src>()
-> impl Parser<'src, &'src [Token], DescribeWindowProcessor, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Describe)
        .ignore_then(kw(Identifier::Window))
        .ignore_then(kw(Identifier::Processor))
        .ignore_then(window_processor_ref())
        .map(|name| DescribeWindowProcessor { name })
        .then_ignore(tok(Token::Semicolon).or_not())
}

pub fn parse_describe_window_processor_tokens(
    tokens: &[Token],
) -> Result<DescribeWindowProcessor, Vec<ParseError<'_>>> {
    let out = describe_window_processor_parser()
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

pub fn parse_describe_window_processor(
    input: &str,
) -> Result<DescribeWindowProcessor, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_describe_window_processor_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_describe_window_processor(input: &str, cursor: usize) -> Vec<String> {
    suggest_from!(input, cursor, describe_window_processor_parser())
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
    fn parses_describe_window_processor() {
        let tokens = to_tokens("DESCRIBE WINDOW PROCESSOR latency_window;");
        let parsed = parse_describe_window_processor_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.name.as_str(), "latency_window");
    }

    #[test]
    fn suggests_window_processor_reference_after_describe_window_processor() {
        let input = "DESCRIBE WINDOW PROCESSOR ";
        let suggestions = suggest_describe_window_processor(input, input.len());
        assert!(suggestions.contains(&"ref:window_processor".to_string()));
    }
}
