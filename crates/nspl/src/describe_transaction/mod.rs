//! `DESCRIBE TRANSACTION` grammar.
//!
//! Layer: language.
//! - **Owns.** Parsing one transaction inspection: its optional identity, selected operation and
//!   report format, in that fixed order.
//! - **Depends on.** Shared NSPL tokens, the shared identity and number primitives, and
//!   vocabulary Models.
//! - **Must not know.** Sessions, transaction bindings, the inspection service, or how a report is
//!   rendered.

use chumsky::prelude::*;
use nervix_models::{
    DescribeTransaction, TransactionInspectionRequest, TransactionInspectionTarget,
};

use crate::{
    lexer::{Identifier, Token, Word},
    parser_support::{
        ParseError, inspection_format, kw, tok, transaction_id, transaction_operation_number,
    },
};

/// `DESCRIBE TRANSACTION [ '<id>' ] [ OPERATION <n> ] [ FORMAT TEXT | JSON ]`.
///
/// Omitting the identity reads the transaction attached to the session, and omitting the format
/// renders `TEXT`.
pub fn describe_transaction_parser<'src>()
-> impl Parser<'src, &'src [Token], DescribeTransaction, extra::Err<ParseError<'src>>> + Clone {
    let target = transaction_id()
        .or_not()
        .map(|transaction_id| match transaction_id {
            Some(transaction_id) => TransactionInspectionTarget::Transaction { transaction_id },
            None => TransactionInspectionTarget::Attached,
        });
    let operation = kw(Identifier::Operation)
        .ignore_then(transaction_operation_number())
        .or_not();
    let format = kw(Identifier::Format)
        .ignore_then(inspection_format())
        .or_not();
    kw(Identifier::Describe)
        .ignore_then(kw(Identifier::Transaction))
        .ignore_then(target)
        .then(operation)
        .then(format)
        .map(|((target, operation), format)| DescribeTransaction {
            request: TransactionInspectionRequest { target, operation },
            format: format.unwrap_or_default(),
        })
        .then_ignore(tok(Token::Semicolon).or_not())
        .boxed()
}

/// The clauses that may still follow a complete `DESCRIBE TRANSACTION`, as completion offers them.
///
/// A statement that already parses leaves no expectations to derive offers from, so its optional
/// tail is named from what was written: a clause may follow only the clauses before it in the
/// fixed order. `FORMAT TEXT` parses to the same Model as an omitted format, so whether a format
/// was written is read from `tokens`, the statement the Model was parsed from.
pub(crate) fn describe_transaction_tail(
    describe: &DescribeTransaction,
    tokens: &[Token],
) -> Vec<String> {
    let format_written = tokens.iter().any(|token| {
        matches!(
            token,
            Token::Word(Word::KnownWord {
                iden: Identifier::Format,
                ..
            })
        )
    });
    if format_written {
        return vec![";".to_string()];
    }
    let mut tail = vec![";".to_string(), "FORMAT".to_string()];
    if describe.request.operation.is_none() {
        tail.push("OPERATION".to_string());
        if describe.request.target == TransactionInspectionTarget::Attached {
            tail.push("transaction_id".to_string());
        }
    }
    // Completion offers are sorted everywhere else, so a caller merging them never reorders.
    tail.sort();
    tail
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use meticulous::OptionExt as _;
    use nervix_models::{
        DescribeTransaction, InspectionFormat, Statement, TransactionInspectionRequest,
        TransactionInspectionTarget, TransactionOperationNumber,
    };
    use rstest::rstest;

    use crate::statement::{parse_statement, suggest_statement};

    fn operation(number: usize) -> TransactionOperationNumber {
        TransactionOperationNumber::new(
            NonZeroUsize::new(number).assured("the test names operations from one"),
        )
    }

    fn describe(
        target: TransactionInspectionTarget,
        operation: Option<TransactionOperationNumber>,
        format: InspectionFormat,
    ) -> Statement {
        Statement::DescribeTransaction(DescribeTransaction {
            request: TransactionInspectionRequest { target, operation },
            format,
        })
    }

    fn named(transaction_id: &str) -> TransactionInspectionTarget {
        TransactionInspectionTarget::Transaction {
            transaction_id: transaction_id.to_string(),
        }
    }

    #[rstest]
    #[case::attached(
        "DESCRIBE TRANSACTION;",
        describe(TransactionInspectionTarget::Attached, None, InspectionFormat::Text)
    )]
    #[case::attached_without_terminator(
        "DESCRIBE TRANSACTION",
        describe(TransactionInspectionTarget::Attached, None, InspectionFormat::Text)
    )]
    #[case::operation(
        "DESCRIBE TRANSACTION OPERATION 2;",
        describe(
            TransactionInspectionTarget::Attached,
            Some(operation(2)),
            InspectionFormat::Text
        )
    )]
    #[case::explicit_text(
        "DESCRIBE TRANSACTION FORMAT TEXT;",
        describe(TransactionInspectionTarget::Attached, None, InspectionFormat::Text)
    )]
    #[case::attached_json(
        "DESCRIBE TRANSACTION FORMAT JSON;",
        describe(TransactionInspectionTarget::Attached, None, InspectionFormat::Json)
    )]
    #[case::identified(
        "DESCRIBE TRANSACTION '0199c1a0-7c1e-7b52';",
        describe(named("0199c1a0-7c1e-7b52"), None, InspectionFormat::Text)
    )]
    #[case::double_quoted_identity(
        "DESCRIBE TRANSACTION \"tx.refresh.3\" FORMAT JSON;",
        describe(named("tx.refresh.3"), None, InspectionFormat::Json)
    )]
    #[case::every_clause(
        "DESCRIBE TRANSACTION 'transaction-id' OPERATION 2 FORMAT JSON;",
        describe(named("transaction-id"), Some(operation(2)), InspectionFormat::Json)
    )]
    #[case::lowercase_keywords(
        "describe transaction 'transaction-id' operation 7 format text;",
        describe(named("transaction-id"), Some(operation(7)), InspectionFormat::Text)
    )]
    #[case::mixed_case_keywords(
        "Describe Transaction OpErAtIoN 1 Format Json",
        describe(
            TransactionInspectionTarget::Attached,
            Some(operation(1)),
            InspectionFormat::Json
        )
    )]
    #[case::spread_over_lines(
        "DESCRIBE\nTRANSACTION\n  'transaction-id'\n  OPERATION 3\n  FORMAT JSON;",
        describe(named("transaction-id"), Some(operation(3)), InspectionFormat::Json)
    )]
    fn parses_every_optional_clause(#[case] source: &str, #[case] expected: Statement) {
        let parsed = parse_statement(source).expect("DESCRIBE TRANSACTION must parse");
        assert_eq!(parsed, expected);
    }

    #[rstest]
    #[case::attached("DESCRIBE TRANSACTION;")]
    #[case::operation("DESCRIBE TRANSACTION OPERATION 2;")]
    #[case::json("DESCRIBE TRANSACTION FORMAT JSON;")]
    #[case::identified("DESCRIBE TRANSACTION 'transaction-id' OPERATION 2 FORMAT JSON;")]
    #[case::quote_in_identity("DESCRIBE TRANSACTION \"it's\";")]
    fn renders_a_canonical_form_that_parses_back(#[case] source: &str) {
        let parsed = parse_statement(source).expect("DESCRIBE TRANSACTION must parse");
        let canonical = parsed
            .to_canonical_nspl()
            .expect("DESCRIBE TRANSACTION always renders");
        assert_eq!(canonical, source);
        let reparsed = parse_statement(&canonical).expect("the canonical form must parse");
        assert_eq!(reparsed, parsed);
    }

    #[test]
    fn the_default_format_is_not_rendered() {
        let parsed = parse_statement("DESCRIBE TRANSACTION OPERATION 1 FORMAT TEXT;")
            .expect("DESCRIBE TRANSACTION must parse");
        assert_eq!(
            parsed
                .to_canonical_nspl()
                .expect("DESCRIBE TRANSACTION always renders"),
            "DESCRIBE TRANSACTION OPERATION 1;"
        );
    }

    #[rstest]
    #[case::zero_operation("DESCRIBE TRANSACTION OPERATION 0;", "operations are numbered from 1")]
    #[case::negative_operation("DESCRIBE TRANSACTION OPERATION -1;", "operation_number")]
    #[case::fractional_operation("DESCRIBE TRANSACTION OPERATION 1.5;", "invalid integer")]
    #[case::word_operation("DESCRIBE TRANSACTION OPERATION first;", "invalid integer")]
    #[case::missing_operation("DESCRIBE TRANSACTION OPERATION;", "operation_number")]
    #[case::overflowing_operation(
        "DESCRIBE TRANSACTION OPERATION 18446744073709551616;",
        "invalid integer"
    )]
    #[case::empty_identity("DESCRIBE TRANSACTION '';", "transaction_id must not be empty")]
    #[case::unquoted_identity("DESCRIBE TRANSACTION transaction_id;", "transaction_id")]
    #[case::numeric_identity("DESCRIBE TRANSACTION 42;", "transaction_id")]
    #[case::unknown_format("DESCRIBE TRANSACTION FORMAT YAML;", "TEXT | JSON")]
    #[case::missing_format("DESCRIBE TRANSACTION FORMAT;", "TEXT | JSON")]
    #[case::format_before_operation(
        "DESCRIBE TRANSACTION FORMAT JSON OPERATION 2;",
        "found OPERATION"
    )]
    #[case::operation_before_identity(
        "DESCRIBE TRANSACTION OPERATION 2 'transaction-id';",
        "transaction-id"
    )]
    #[case::format_before_identity(
        "DESCRIBE TRANSACTION FORMAT JSON 'transaction-id';",
        "transaction-id"
    )]
    #[case::repeated_operation("DESCRIBE TRANSACTION OPERATION 1 OPERATION 2;", "found OPERATION")]
    #[case::repeated_format("DESCRIBE TRANSACTION FORMAT TEXT FORMAT JSON;", "found FORMAT")]
    #[case::two_identities("DESCRIBE TRANSACTION 'first' 'second';", "second")]
    #[case::plural_keyword("DESCRIBE TRANSACTIONS;", "TRANSACTIONS")]
    fn rejects_a_malformed_inspection(#[case] source: &str, #[case] expected: &str) {
        let error = parse_statement(source).expect_err("the inspection is malformed");
        assert!(
            error.to_string().contains(expected),
            "{source:?} must report {expected:?}, got {error}"
        );
    }

    #[test]
    fn describe_offers_the_transaction_among_its_targets() {
        let suggestions = suggest_statement("DESCRIBE ", "DESCRIBE ".len());
        assert!(
            suggestions.contains(&"TRANSACTION".to_string()),
            "{suggestions:?}"
        );
        assert!(
            suggestions.contains(&"RESOURCE".to_string()),
            "{suggestions:?}"
        );

        let typed = "DESCRIBE TRANS";
        assert_eq!(
            suggest_statement(typed, typed.len()),
            vec!["TRANSACTION".to_string()]
        );
    }

    /// Each clause is offered only where it may still be written, in its fixed order, and nothing
    /// from another `DESCRIBE` target or another statement leaks into the offers.
    #[rstest]
    #[case::after_keyword(
        "DESCRIBE TRANSACTION ",
        &[";", "FORMAT", "OPERATION", "transaction_id"]
    )]
    #[case::after_identity("DESCRIBE TRANSACTION 'transaction-id' ", &[";", "FORMAT", "OPERATION"])]
    #[case::after_operation("DESCRIBE TRANSACTION OPERATION 2 ", &[";", "FORMAT"])]
    #[case::after_identified_operation(
        "DESCRIBE TRANSACTION 'transaction-id' OPERATION 2 ",
        &[";", "FORMAT"]
    )]
    #[case::after_default_format("DESCRIBE TRANSACTION FORMAT TEXT ", &[";"])]
    #[case::after_json_format("DESCRIBE TRANSACTION OPERATION 2 FORMAT JSON ", &[";"])]
    #[case::operation_number("DESCRIBE TRANSACTION OPERATION ", &["operation_number"])]
    #[case::report_format("DESCRIBE TRANSACTION 'transaction-id' FORMAT ", &["JSON", "TEXT"])]
    #[case::terminated("DESCRIBE TRANSACTION FORMAT JSON; ", &[])]
    fn offers_only_the_clauses_that_may_follow(#[case] source: &str, #[case] expected: &[&str]) {
        let suggestions = suggest_statement(source, source.len());
        let expected = expected
            .iter()
            .map(|suggestion| suggestion.to_string())
            .collect::<Vec<_>>();
        assert_eq!(suggestions, expected, "{source:?}");
    }

    #[rstest]
    #[case::operation("DESCRIBE TRANSACTION OP", &["OPERATION"])]
    #[case::format_after_operation("DESCRIBE TRANSACTION OPERATION 2 F", &["FORMAT"])]
    #[case::json("DESCRIBE TRANSACTION FORMAT J", &["JSON"])]
    #[case::text("DESCRIBE TRANSACTION FORMAT t", &["TEXT"])]
    fn a_typed_prefix_keeps_its_clause(#[case] source: &str, #[case] expected: &[&str]) {
        let suggestions = suggest_statement(source, source.len());
        let expected = expected
            .iter()
            .map(|suggestion| suggestion.to_string())
            .collect::<Vec<_>>();
        assert_eq!(suggestions, expected, "{source:?}");
    }

    /// The transaction keywords belong to this statement alone.
    #[test]
    fn transaction_keywords_do_not_leak_into_other_statements() {
        for source in [
            "DESCRIBE RESOURCE bundle ",
            "SHOW ",
            "CREATE ",
            "CREATE SIGNALING PROTOCOL p FORMAT ",
        ] {
            let suggestions = suggest_statement(source, source.len());
            for keyword in ["TRANSACTION", "OPERATION", "TEXT", "transaction_id"] {
                assert!(
                    !suggestions.contains(&keyword.to_string()),
                    "{source:?} must not offer {keyword}, got {suggestions:?}"
                );
            }
        }
    }
}
