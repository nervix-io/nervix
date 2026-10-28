//! Configured-client inspection grammar.
//!
//! Layer: language.
//! - **Owns.** The DESCRIBE CLIENT statement family and its semantic body.
//! - **Depends on.** Shared language tokens, client references and vocabulary Models.
//! - **Must not know.** Client drivers, registry reads or runtime configuration.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns configured-client inspection grammar")
)]

use chumsky::prelude::*;
use nervix_models::DescribeClient;

use crate::{
    lexer::{Identifier, Token},
    parser_support::{ParseError, client_ref, kw, tok},
};

pub fn describe_client_parser<'src>()
-> impl Parser<'src, &'src [Token], DescribeClient, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Describe)
        .ignore_then(kw(Identifier::Client))
        .ignore_then(client_ref())
        .map(|name| DescribeClient { name })
        .then_ignore(tok(Token::Semicolon).or_not())
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns composed client-inspection grammar evidence")
)]
mod tests {
    //! Layer: test harness.
    //! - **Owns.** Parsing, canonical rendering and completion checks of client inspection.
    //! - **Depends on.** The composed grammar and public semantic Models.
    //! - **Must not know.** Registry storage, drivers or execution.

    use meticulous::ResultExt as _;
    use nervix_models::{ClientName, DescribeClient, Statement};

    use crate::statement::{parse_statement, suggest_statement};

    #[test]
    fn parses_client_inspection_through_the_composed_grammar() {
        for source in ["DESCRIBE CLIENT warehouse;", "describe client warehouse"] {
            let statement = parse_statement(source).assured("client inspection parses");
            assert_eq!(
                statement,
                Statement::DescribeClient(DescribeClient {
                    name: ClientName::parse("warehouse")
                        .assured("warehouse is a valid client name"),
                })
            );
            assert_eq!(
                statement
                    .to_canonical_plexus()
                    .assured("client inspection has canonical text"),
                "DESCRIBE CLIENT warehouse;"
            );
            assert!(!statement.requires_domain_mutation_ownership());
        }
    }

    #[test]
    fn client_inspection_requires_exactly_one_client_reference() {
        for source in [
            "DESCRIBE CLIENT;",
            "DESCRIBE CLIENT FROM warehouse;",
            "DESCRIBE CLIENT warehouse extra;",
        ] {
            assert!(parse_statement(source).is_err(), "{source}");
        }
    }

    #[test]
    fn composed_completion_expects_a_client_reference() {
        let source = "DESCRIBE CLIENT ";
        assert_eq!(suggest_statement(source, source.len()), ["ref:client"]);
        let source = "DESCRIBE ";
        assert!(suggest_statement(source, source.len()).contains(&"CLIENT".to_string()));
    }
}
