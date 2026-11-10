//! The cluster licensing inspection statement.
//!
//! Layer: language.
//! - **Owns.** Parsing and completion of DESCRIBE LICENSE into its semantic Model.
//! - **Depends on.** Shared keyword tokens, grammar primitives and the statement vocabulary.
//! - **Must not know.** Policies, records, admission or runtime execution.

use chumsky::prelude::*;
use meticulous::OptionExt as _;
use nervix_models::DescribeLicense;

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        LexedInput, ParseError, ParseFromSourceError, into_parse_error, kw, lex_input,
        suggest_from, tok,
    },
};

pub fn describe_license_parser<'src>()
-> impl Parser<'src, &'src [Token], DescribeLicense, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Describe)
        .ignore_then(kw(Identifier::License))
        .to(DescribeLicense)
        .then_ignore(tok(Token::Semicolon).or_not())
}

pub fn parse_describe_license_tokens(
    tokens: &[Token],
) -> Result<DescribeLicense, Vec<ParseError<'_>>> {
    let out = describe_license_parser().then_ignore(end()).parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .verified("has_errors returned false above, so this parse produced output"))
    }
}

pub fn parse_describe_license(
    input: &str,
) -> error_stack::Result<DescribeLicense, ParseFromSourceError> {
    let LexedInput {
        source,
        spanned_tokens,
        tokens,
    } = lex_input(input)?;
    parse_describe_license_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_describe_license(input: &str, cursor: usize) -> Vec<String> {
    suggest_from!(input, cursor, describe_license_parser())
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
    fn parses_describe_license_in_any_case() {
        for input in [
            "DESCRIBE LICENSE;",
            "describe license;",
            "DeScRiBe LiCeNsE;",
            "DESCRIBE LICENSE",
        ] {
            let tokens = to_tokens(input);
            assert_eq!(
                parse_describe_license_tokens(&tokens),
                Ok(DescribeLicense),
                "{input}"
            );
        }
    }

    #[test]
    fn rejects_a_named_or_qualified_license() {
        for input in [
            "DESCRIBE LICENSE prod;",
            "DESCRIBE LICENSE STATUS;",
            "DESCRIBE;",
        ] {
            let tokens = to_tokens(input);
            assert!(parse_describe_license_tokens(&tokens).is_err(), "{input}");
        }
        assert!(parse_describe_license("DESCRIBE LICENSE node-1;").is_err());
    }

    #[test]
    fn suggests_license_after_describe() {
        let input = "DESCRIBE ";
        let suggestions = suggest_describe_license(input, input.len());
        assert!(suggestions.contains(&"LICENSE".to_string()));
        assert!(!suggestions.contains(&"STATUS".to_string()));
    }

    #[test]
    fn suggests_nothing_but_the_end_after_describe_license() {
        let input = "DESCRIBE LICENSE ";
        let suggestions = suggest_describe_license(input, input.len());
        assert!(!suggestions.contains(&"STATUS".to_string()));
        assert!(!suggestions.contains(&"LICENSE".to_string()));
    }

    #[test]
    fn composed_statement_grammar_keeps_the_cluster_license_head_complete() {
        let statement = crate::statement::parse_statement("DESCRIBE LICENSE;")
            .expect("the composed grammar selects the licensing statement");
        assert_eq!(
            statement,
            nervix_models::Statement::DescribeLicense(DescribeLicense)
        );
        let head = "DESCRIBE ";
        assert!(
            crate::statement::suggest_statement(head, head.len()).contains(&"LICENSE".to_string())
        );
        let complete = "DESCRIBE LICENSE ";
        assert!(crate::statement::suggest_statement(complete, complete.len()).is_empty());
        for input in [
            "DESCRIBE LICENSE CLUSTER;",
            "DESCRIBE LICENSE example;",
            "DESCRIBE LICENSE STATUS;",
        ] {
            assert!(crate::statement::parse_statement(input).is_err(), "{input}");
        }
    }
}
