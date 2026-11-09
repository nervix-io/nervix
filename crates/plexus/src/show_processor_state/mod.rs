//! `SHOW REORDERER <name> STATE` and `SHOW CORRELATOR <name> STATE`.
//!
//! Layer: language.
//! - **Owns.** The statement head that names an inspected processor by its kind and name.
//! - **Depends on.** Shared Plexus tokens, typed reference parsers and the shared state inspection
//!   clauses.
//! - **Must not know.** Branch tasks, the rows they hold or inspection pages.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the boundary declared above")
)]

use chumsky::prelude::*;
use meticulous::OptionExt as _;
use nervix_models::{ShowProcessorState, StateInspectedProcessor};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        LexedInput, ParseError, ParseFromSourceError, correlator_ref, into_parse_error, kw,
        lex_input, reorderer_ref, suggest_from,
    },
    state_inspection_clauses::state_inspection_clauses,
};

pub fn show_processor_state_parser<'src>()
-> impl Parser<'src, &'src [Token], ShowProcessorState, extra::Err<ParseError<'src>>> + Clone {
    let processor = choice((
        kw(Identifier::Reorderer)
            .ignore_then(reorderer_ref())
            .map(StateInspectedProcessor::Reorderer),
        kw(Identifier::Correlator)
            .ignore_then(correlator_ref())
            .map(StateInspectedProcessor::Correlator),
    ));
    kw(Identifier::Show)
        .ignore_then(processor)
        .then_ignore(kw(Identifier::State))
        .then(state_inspection_clauses())
        .map(|(processor, clauses)| ShowProcessorState {
            processor,
            selection: clauses.selection,
            page: clauses.page,
            format: clauses.format,
        })
}

pub fn parse_show_processor_state_tokens(
    tokens: &[Token],
) -> Result<ShowProcessorState, Vec<ParseError<'_>>> {
    let out = show_processor_state_parser()
        .then_ignore(end())
        .parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .verified("has_errors returned false above, so this parse produced output"))
    }
}

pub fn parse_show_processor_state(
    input: &str,
) -> error_stack::Result<ShowProcessorState, ParseFromSourceError> {
    let LexedInput {
        source,
        spanned_tokens,
        tokens,
    } = lex_input(input)?;
    parse_show_processor_state_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_show_processor_state(input: &str, cursor: usize) -> Vec<String> {
    suggest_from!(input, cursor, show_processor_state_parser())
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;
    use nervix_models::{InspectionFormat, StateBranchSelection, Statement};

    use super::*;
    use crate::statement::{parse_statement, suggest_statement};

    #[test]
    fn both_processor_kinds_name_their_state_with_every_clause() {
        for (head, kind) in [
            ("SHOW REORDERER held STATE", "reorderer"),
            ("SHOW CORRELATOR held STATE", "correlator"),
        ] {
            for selector in [
                "FOR ALL BRANCHES",
                "FOR UNBRANCHED",
                "FOR BRANCH VALUES { tenant = 'alpha', shard = -7 }",
            ] {
                let text = format!(
                    "{head} {selector} LIMIT 3 AFTER $cursor$opaque'position$cursor$ FORMAT JSON;"
                );
                let statement =
                    parse_statement(&text).assured("the clauses are in their declared order");
                let Statement::ShowProcessorState(show) = &statement else {
                    panic!("a processor state inspection: {statement:?}");
                };
                match (&show.processor, kind) {
                    (StateInspectedProcessor::Reorderer(name), "reorderer") => {
                        assert_eq!(name.as_str(), "held");
                    }
                    (StateInspectedProcessor::Correlator(name), "correlator") => {
                        assert_eq!(name.as_str(), "held");
                    }
                    (other, _) => panic!("the head names its kind: {other:?}"),
                }
                assert_eq!(show.format, InspectionFormat::Json);
                assert!(show.selection.is_some());
                let page = show.page.as_ref().assured("a page was requested");
                assert_eq!(page.limit.get(), 3);
                assert_eq!(page.after.as_deref(), Some("opaque'position"));
                let canonical = statement
                    .to_canonical_plexus()
                    .assured("literal clauses render");
                assert_eq!(
                    parse_statement(&canonical).assured("canonical grammar"),
                    statement
                );
            }
        }
        let show = parse_show_processor_state("show correlator settle state for unbranched;")
            .assured("keywords are case insensitive");
        assert_eq!(show.selection, Some(StateBranchSelection::Unbranched));
        assert_eq!(show.format, InspectionFormat::Text);
        assert!(show.page.is_none());
    }

    #[test]
    fn malformed_processor_state_reads_are_refused() {
        for statement in [
            "SHOW REORDERER STATE;",
            "SHOW REORDERER held;",
            "SHOW REORDERER held MATERIALIZED STATE;",
            "SHOW CORRELATOR held STATE LIMIT 0;",
            "SHOW CORRELATOR held STATE LIMIT -1;",
            "SHOW CORRELATOR held STATE AFTER 'position';",
            "SHOW REORDERER held STATE LIMIT 1 AFTER 3;",
            "SHOW REORDERER held STATE FOR BRANCH VALUES {};",
            "SHOW REORDERER held STATE FORMAT JSON LIMIT 1;",
            "SHOW REORDERER held STATE FOR UNBRANCHED FOR ALL BRANCHES;",
            "SHOW REORDERER held STATE FORMAT XML;",
            "SHOW JUNCTION held STATE;",
            "SHOW DEDUPLICATOR held STATE;",
        ] {
            assert!(parse_statement(statement).is_err(), "{statement}");
        }
    }

    #[test]
    fn completion_offers_the_processor_kinds_and_their_state_clauses() {
        let input = "SHOW ";
        let suggestions = suggest_statement(input, input.len());
        for expected in ["REORDERER", "CORRELATOR", "RELAY"] {
            assert!(
                suggestions.iter().any(|item| item == expected),
                "{expected}: {suggestions:?}"
            );
        }
        let input = "SHOW REORDERER ";
        assert!(
            suggest_show_processor_state(input, input.len()).contains(&"ref:reorderer".to_string())
        );
        let input = "SHOW CORRELATOR ";
        assert!(
            suggest_show_processor_state(input, input.len())
                .contains(&"ref:correlator".to_string())
        );
        let input = "SHOW CORRELATOR settle ";
        assert!(suggest_show_processor_state(input, input.len()).contains(&"STATE".to_string()));
        for (prefix, expected) in [
            ("SHOW REORDERER held STATE FOR ", "ALL BRANCHES"),
            ("SHOW CORRELATOR held STATE FOR ", "BRANCH VALUES"),
            ("SHOW REORDERER held STATE ", "LIMIT"),
            ("SHOW CORRELATOR held STATE FORMAT ", "JSON"),
            ("SHOW REORDERER held STATE LIMIT 1 ", "AFTER"),
        ] {
            let suggestions = suggest_statement(prefix, prefix.len());
            assert!(
                suggestions.iter().any(|item| item == expected),
                "{prefix}: {suggestions:?}"
            );
            assert!(
                !suggestions
                    .iter()
                    .any(|item| item == "CREATE" || item == "MATERIALIZED"),
                "{prefix}: {suggestions:?}"
            );
        }
    }
}
