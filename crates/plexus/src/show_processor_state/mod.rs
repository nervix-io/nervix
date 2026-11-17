//! Kinded `SHOW <processor> <name> STATE` statements.
//!
//! Layer: language.
//! - **Owns.** The statement heads that name an inspected processor by its kind and name, the WASM
//!   processor's checkpoint-fact read among them.
//! - **Depends on.** Shared Plexus tokens, typed reference parsers and the shared state inspection
//!   clauses.
//! - **Must not know.** Branch tasks, the rows they hold or inspection pages.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the boundary declared above")
)]

use chumsky::prelude::*;
use meticulous::OptionExt as _;
use nervix_models::{ShowProcessorState, ShowWasmProcessorState, StateInspectedProcessor};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        LexedInput, ParseError, ParseFromSourceError, correlator_ref, deduplicator_ref,
        inferencer_ref, into_parse_error, kw, kw_phrase2, lex_input, reorderer_ref, suggest_from,
        wasm_processor_ref, window_processor_ref,
    },
    state_inspection_clauses::state_inspection_clauses,
};

pub fn show_processor_state_parser<'src>()
-> impl Parser<'src, &'src [Token], ShowProcessorState, extra::Err<ParseError<'src>>> + Clone {
    let processor = choice((
        kw(Identifier::Inferencer)
            .ignore_then(inferencer_ref())
            .map(StateInspectedProcessor::Inferencer),
        kw(Identifier::Reorderer)
            .ignore_then(reorderer_ref())
            .map(StateInspectedProcessor::Reorderer),
        kw(Identifier::Correlator)
            .ignore_then(correlator_ref())
            .map(StateInspectedProcessor::Correlator),
        kw(Identifier::Deduplicator)
            .ignore_then(deduplicator_ref())
            .map(StateInspectedProcessor::Deduplicator),
        kw_phrase2(Identifier::Window, Identifier::Processor)
            .ignore_then(window_processor_ref())
            .map(StateInspectedProcessor::WindowProcessor),
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

pub fn show_wasm_processor_state_parser<'src>()
-> impl Parser<'src, &'src [Token], ShowWasmProcessorState, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Show)
        .ignore_then(kw_phrase2(Identifier::Wasm, Identifier::Processor))
        .ignore_then(wasm_processor_ref())
        .then_ignore(kw(Identifier::State))
        .then(state_inspection_clauses())
        .map(|(processor, clauses)| ShowWasmProcessorState {
            processor,
            selection: clauses.selection,
            page: clauses.page,
            format: clauses.format,
        })
}

pub fn parse_show_wasm_processor_state_tokens(
    tokens: &[Token],
) -> Result<ShowWasmProcessorState, Vec<ParseError<'_>>> {
    let out = show_wasm_processor_state_parser()
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

pub fn parse_show_wasm_processor_state(
    input: &str,
) -> error_stack::Result<ShowWasmProcessorState, ParseFromSourceError> {
    let LexedInput {
        source,
        spanned_tokens,
        tokens,
    } = lex_input(input)?;
    parse_show_wasm_processor_state_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_show_wasm_processor_state(input: &str, cursor: usize) -> Vec<String> {
    suggest_from!(input, cursor, show_wasm_processor_state_parser())
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;
    use nervix_models::{InspectionFormat, StateBranchSelection, Statement};

    use super::*;
    use crate::statement::{parse_statement, suggest_statement};

    #[test]
    fn processor_kinds_name_their_state_with_every_clause() {
        for (head, kind) in [
            ("SHOW INFERENCER held STATE", "inferencer"),
            ("SHOW REORDERER held STATE", "reorderer"),
            ("SHOW CORRELATOR held STATE", "correlator"),
            ("SHOW DEDUPLICATOR held STATE", "deduplicator"),
            ("SHOW WINDOW PROCESSOR held STATE", "window_processor"),
        ] {
            for selector in [
                "FOR ALL BRANCHES",
                "FOR UNBRANCHED",
                "FOR BRANCH KEY { tenant = 'alpha', shard = -7 }",
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
                    (StateInspectedProcessor::Inferencer(name), "inferencer") => {
                        assert_eq!(name.as_str(), "held");
                    }
                    (StateInspectedProcessor::Reorderer(name), "reorderer") => {
                        assert_eq!(name.as_str(), "held");
                    }
                    (StateInspectedProcessor::Correlator(name), "correlator") => {
                        assert_eq!(name.as_str(), "held");
                    }
                    (StateInspectedProcessor::Deduplicator(name), "deduplicator") => {
                        assert_eq!(name.as_str(), "held");
                    }
                    (StateInspectedProcessor::WindowProcessor(name), "window_processor") => {
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
            "SHOW INFERENCER STATE;",
            "SHOW INFERENCER held;",
            "SHOW INFERENCER held STATE LIMIT 0;",
            "SHOW INFERENCER held STATE FORMAT JSON LIMIT 1;",
            "SHOW REORDERER STATE;",
            "SHOW REORDERER held;",
            "SHOW REORDERER held MATERIALIZED STATE;",
            "SHOW CORRELATOR held STATE LIMIT 0;",
            "SHOW CORRELATOR held STATE LIMIT -1;",
            "SHOW CORRELATOR held STATE AFTER 'position';",
            "SHOW REORDERER held STATE LIMIT 1 AFTER 3;",
            "SHOW REORDERER held STATE FOR BRANCH KEY {};",
            "SHOW REORDERER held STATE FORMAT JSON LIMIT 1;",
            "SHOW REORDERER held STATE FOR UNBRANCHED FOR ALL BRANCHES;",
            "SHOW REORDERER held STATE FORMAT XML;",
            "SHOW JUNCTION held STATE;",
            "SHOW WINDOW held STATE;",
            "SHOW WINDOW PROCESSOR held STATE LIMIT 0;",
            "SHOW DEDUPLICATOR held STATE FORMAT JSON LIMIT 1;",
        ] {
            assert!(parse_statement(statement).is_err(), "{statement}");
        }
    }

    #[test]
    fn completion_offers_the_processor_kinds_and_their_state_clauses() {
        let input = "SHOW ";
        let suggestions = suggest_statement(input, input.len());
        for expected in [
            "INFERENCER",
            "REORDERER",
            "CORRELATOR",
            "RELAY",
            "DEDUPLICATOR",
            "WINDOW PROCESSOR",
        ] {
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
        for (input, expected) in [
            ("SHOW INFERENCER ", "ref:inferencer"),
            ("SHOW DEDUPLICATOR ", "ref:deduplicator"),
            ("SHOW WINDOW PROCESSOR ", "ref:window_processor"),
        ] {
            assert!(suggest_statement(input, input.len()).contains(&expected.to_string()));
        }
        let input = "SHOW CORRELATOR settle ";
        assert!(suggest_show_processor_state(input, input.len()).contains(&"STATE".to_string()));
        for (prefix, expected) in [
            ("SHOW REORDERER held STATE FOR ", "ALL BRANCHES"),
            ("SHOW CORRELATOR held STATE FOR ", "BRANCH KEY"),
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

#[cfg(test)]
mod wasm_state_tests {
    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_models::{InspectionFormat, StateBranchSelection, Statement};

    use super::{parse_show_wasm_processor_state, suggest_show_wasm_processor_state};
    use crate::statement::{parse_statement, suggest_statement};

    #[test]
    fn wasm_processor_state_reads_take_every_shared_clause() {
        for selector in [
            "FOR ALL BRANCHES",
            "FOR UNBRANCHED",
            "FOR BRANCH KEY { tenant = 'alpha', shard = -7 }",
        ] {
            let text = format!(
                "SHOW WASM PROCESSOR counting_guest STATE {selector} LIMIT 2 AFTER \
                 'opaque-position' FORMAT JSON;"
            );
            let statement =
                parse_statement(&text).assured("the clauses are in their declared order");
            let Statement::ShowWasmProcessorState(show) = &statement else {
                panic!("a WASM processor state read");
            };
            assert_eq!(show.processor.as_str(), "counting_guest");
            assert_eq!(show.format, InspectionFormat::Json);
            assert!(show.selection.is_some());
            let page = show.page.as_ref().assured("a page was requested");
            assert_eq!(page.limit.get(), 2);
            assert_eq!(page.after.as_deref(), Some("opaque-position"));
            let canonical = statement
                .to_canonical_plexus()
                .assured("literal clauses render");
            assert_eq!(
                parse_statement(&canonical).assured("canonical grammar"),
                statement
            );
        }
        let show = parse_show_wasm_processor_state("show wasm processor counting_guest state;")
            .assured("keywords are case insensitive");
        assert_eq!(show.selection, None);
        assert_eq!(show.page, None);
        assert_eq!(show.format, InspectionFormat::Text);
        let show = parse_show_wasm_processor_state(
            "SHOW WASM PROCESSOR counting_guest STATE FOR UNBRANCHED FORMAT TEXT;",
        )
        .assured("an unbranched selection with an explicit text format");
        assert_eq!(show.selection, Some(StateBranchSelection::Unbranched));
        assert_eq!(show.format, InspectionFormat::Text);
    }

    #[test]
    fn malformed_wasm_state_reads_are_refused() {
        for suffix in [
            "LIMIT 0",
            "LIMIT -1",
            "AFTER 'position'",
            "LIMIT 1 AFTER 3",
            "FOR BRANCH KEY {}",
            "FORMAT JSON LIMIT 1",
            "FOR UNBRANCHED FOR ALL BRANCHES",
            "FORMAT XML",
        ] {
            assert!(
                parse_statement(&format!(
                    "SHOW WASM PROCESSOR counting_guest STATE {suffix};"
                ))
                .is_err(),
                "{suffix}"
            );
        }
        for statement in [
            "SHOW WASM PROCESSOR counting_guest;",
            "SHOW WASM PROCESSOR counting_guest MATERIALIZED STATE;",
            "SHOW WASM counting_guest STATE;",
            "SHOW PROCESSOR counting_guest STATE;",
        ] {
            assert!(parse_statement(statement).is_err(), "{statement}");
        }
    }

    #[test]
    fn completion_offers_the_wasm_state_head_and_its_clauses_without_leakage() {
        let head = suggest_statement("SHOW ", "SHOW ".len());
        assert!(
            head.iter().any(|item| item == "WASM PROCESSOR"),
            "SHOW offers the composed WASM PROCESSOR phrase: {head:?}"
        );
        for (prefix, expected) in [
            ("SHOW WASM PROCESSOR ", "ref:wasm_processor"),
            ("SHOW WASM PROCESSOR counting_guest ", "STATE"),
            ("SHOW WASM PROCESSOR counting_guest STATE ", "FOR"),
            ("SHOW WASM PROCESSOR counting_guest STATE ", "LIMIT"),
            ("SHOW WASM PROCESSOR counting_guest STATE ", "FORMAT"),
            (
                "SHOW WASM PROCESSOR counting_guest STATE FOR ",
                "ALL BRANCHES",
            ),
            (
                "SHOW WASM PROCESSOR counting_guest STATE FOR ",
                "BRANCH KEY",
            ),
            ("SHOW WASM PROCESSOR counting_guest STATE FORMAT ", "JSON"),
            ("SHOW WASM PROCESSOR counting_guest STATE LIMIT 1 ", "AFTER"),
        ] {
            let suggestions = suggest_statement(prefix, prefix.len());
            assert!(
                suggestions.iter().any(|item| item == expected),
                "{prefix}: {suggestions:?}"
            );
            assert!(
                !suggestions
                    .iter()
                    .any(|item| item == "CREATE" || item == "RESET" || item == "MATERIALIZED"),
                "{prefix}: {suggestions:?}"
            );
        }
        let input = "SHOW WASM PROCESSOR counting_guest ";
        assert_eq!(
            suggest_show_wasm_processor_state(input, input.len()),
            vec!["STATE".to_string()]
        );
    }
}
