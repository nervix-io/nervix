//! `DESCRIBE BRANCH <branch>`: a branch's declaration and its partition epoch.

use chumsky::prelude::*;
use meticulous::OptionExt as _;
use nervix_models::DescribeBranch;

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        LexedInput, ParseError, ParseFromSourceError, branch_ref, into_parse_error, kw, lex_input,
        suggest_from, tok,
    },
};

pub fn describe_branch_parser<'src>()
-> impl Parser<'src, &'src [Token], DescribeBranch, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Describe)
        .ignore_then(kw(Identifier::Branch))
        .ignore_then(branch_ref())
        .map(|branch| DescribeBranch { branch })
        .then_ignore(tok(Token::Semicolon).or_not())
}

pub fn parse_describe_branch_tokens(
    tokens: &[Token],
) -> Result<DescribeBranch, Vec<ParseError<'_>>> {
    let out = describe_branch_parser().then_ignore(end()).parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .verified("has_errors returned false above, so this parse produced output"))
    }
}

pub fn parse_describe_branch(
    input: &str,
) -> error_stack::Result<DescribeBranch, ParseFromSourceError> {
    let LexedInput {
        source,
        spanned_tokens,
        tokens,
    } = lex_input(input)?;
    parse_describe_branch_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_describe_branch(input: &str, cursor: usize) -> Vec<String> {
    suggest_from!(input, cursor, describe_branch_parser())
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
    fn parses_describe_branch() {
        let tokens = to_tokens("DESCRIBE BRANCH by_tenant;");
        let parsed = parse_describe_branch_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.branch.as_str(), "by_tenant");
    }

    #[test]
    fn parses_describe_branch_in_any_keyword_case_without_a_semicolon() {
        let parsed =
            parse_describe_branch("describe Branch by_tenant").expect("parse should succeed");
        assert_eq!(parsed.branch.as_str(), "by_tenant");
    }

    #[test]
    fn refuses_describe_branch_without_a_branch() {
        let error = parse_describe_branch("DESCRIBE BRANCH;").expect_err("a branch is required");
        assert!(format!("{error:#}").contains("branch"), "{error:#}");
    }

    #[test]
    fn suggests_a_branch_reference_after_describe_branch() {
        let input = "DESCRIBE BRANCH ";
        let suggestions = suggest_describe_branch(input, input.len());
        assert!(
            suggestions.contains(&"ref:branch".to_string()),
            "{suggestions:?}"
        );
    }
}
