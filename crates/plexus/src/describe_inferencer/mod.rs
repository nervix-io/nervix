//! Layer: language.
//! Owns: inferencer description grammar and its completion expectations.
//! Depends on: shared tokens, reference grammar and semantic Models.
//! Must not know: registry validation, inference execution or state observation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "parses inferencer descriptions")
)]

use chumsky::prelude::*;
use meticulous::OptionExt as _;
use nervix_models::DescribeInferencer;

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        LexedInput, ParseError, ParseFromSourceError, inferencer_ref, into_parse_error, kw,
        lex_input, suggest_from, tok,
    },
};

pub fn describe_inferencer_parser<'src>()
-> impl Parser<'src, &'src [Token], DescribeInferencer, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Describe)
        .ignore_then(kw(Identifier::Inferencer))
        .ignore_then(inferencer_ref())
        .map(|name| DescribeInferencer { name })
        .then_ignore(tok(Token::Semicolon).or_not())
}

pub fn parse_describe_inferencer_tokens(
    tokens: &[Token],
) -> Result<DescribeInferencer, Vec<ParseError<'_>>> {
    let out = describe_inferencer_parser()
        .then_ignore(end())
        .parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .verified("a successful parse produced its Model"))
    }
}

pub fn parse_describe_inferencer(
    input: &str,
) -> error_stack::Result<DescribeInferencer, ParseFromSourceError> {
    let LexedInput {
        source,
        spanned_tokens,
        tokens,
    } = lex_input(input)?;
    parse_describe_inferencer_tokens(&tokens)
        .map_err(|errors| into_parse_error(source, &spanned_tokens, input.len(), errors))
}

pub fn suggest_describe_inferencer(input: &str, cursor: usize) -> Vec<String> {
    suggest_from!(input, cursor, describe_inferencer_parser())
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;

    use super::*;

    #[test]
    fn descriptions_keep_the_inferencer_identity_with_case_insensitive_keywords() {
        for source in [
            "DESCRIBE INFERENCER scoring;",
            "describe inferencer scoring",
        ] {
            let description = parse_describe_inferencer(source).assured("the description parses");
            assert_eq!(description.name.as_str(), "scoring");
        }
    }

    #[test]
    fn descriptions_require_one_inferencer_reference_and_no_other_statement_body() {
        for source in [
            "DESCRIBE INFERENCER;",
            "DESCRIBE INFERENCER scoring extra;",
            "DESCRIBE WINDOW PROCESSOR scoring;",
        ] {
            assert!(parse_describe_inferencer(source).is_err(), "{source}");
        }
    }

    #[test]
    fn completion_follows_the_inferencer_head_and_reference() {
        use crate::statement::suggest_statement;

        let head = "DESCRIBE ";
        let suggestions = suggest_statement(head, head.len());
        assert!(
            suggestions
                .iter()
                .any(|suggestion| suggestion == "INFERENCER")
        );
        let reference = "DESCRIBE INFERENCER ";
        assert_eq!(
            suggest_statement(reference, reference.len()),
            vec!["ref:inferencer".to_owned()],
        );
        let other_head = "DESCRIBE WINDOW PROCESSOR ";
        assert_eq!(
            suggest_statement(other_head, other_head.len()),
            vec!["ref:window_processor".to_owned()],
        );
    }
}
