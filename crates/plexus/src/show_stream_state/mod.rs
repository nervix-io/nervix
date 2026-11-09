//! `SHOW ... STATE` grammars and the inspection clauses every state read shares.
//!
//! Layer: language.
//! - **Owns.** The relay and WASM processor state read heads, the shared selector, page and format
//!   clauses, and their completion.
//! - **Depends on.** Shared Plexus tokens, reference grammar and inspection vocabulary Models.
//! - **Must not know.** State storage, owner routing, envelopes or rendering.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use chumsky::prelude::*;
use meticulous::OptionExt as _;
use nervix_models::{
    InspectionFormat, ShowRelayMaterializedState, ShowWasmProcessorState, StateBranchSelection,
    StateInspectionPageRequest,
};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        LexedInput, ParseError, ParseFromSourceError, inspection_format, into_parse_error, kw,
        kw_phrase2, lex_input, nonzero_u64_value, relay_ref, string_lit, suggest_from, tok,
        wasm_processor_ref,
    },
};

/// The selector, page and format clauses of one state read, in their only written order.
#[derive(Clone)]
struct StateInspectionClauses {
    selection: Option<StateBranchSelection>,
    page: Option<StateInspectionPageRequest>,
    format: InspectionFormat,
}

fn state_inspection_clauses<'src>()
-> impl Parser<'src, &'src [Token], StateInspectionClauses, extra::Err<ParseError<'src>>> + Clone {
    let page = kw(Identifier::Limit)
        .ignore_then(nonzero_u64_value("row_limit", "LIMIT must be positive"))
        .then(kw(Identifier::After).ignore_then(string_lit()).or_not())
        .map(|(limit, after)| StateInspectionPageRequest { limit, after });
    crate::state_branch_selection::branch_selection()
        .or_not()
        .then(page.or_not())
        .then(
            kw(Identifier::Format)
                .ignore_then(inspection_format())
                .or_not(),
        )
        .then_ignore(tok(Token::Semicolon).or_not())
        .map(|((selection, page), format)| StateInspectionClauses {
            selection,
            page,
            format: format.unwrap_or(InspectionFormat::Text),
        })
}

pub fn show_stream_materialized_state_parser<'src>()
-> impl Parser<'src, &'src [Token], ShowRelayMaterializedState, extra::Err<ParseError<'src>>> + Clone
{
    kw(Identifier::Show)
        .ignore_then(kw(Identifier::Relay))
        .ignore_then(relay_ref())
        .then_ignore(kw(Identifier::Materialized))
        .then_ignore(kw(Identifier::State))
        .then(state_inspection_clauses())
        .map(|(relay, clauses)| ShowRelayMaterializedState {
            relay,
            selection: clauses.selection,
            page: clauses.page,
            format: clauses.format,
        })
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

pub fn parse_show_stream_materialized_state_tokens(
    tokens: &[Token],
) -> Result<ShowRelayMaterializedState, Vec<ParseError<'_>>> {
    let out = show_stream_materialized_state_parser()
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

pub fn parse_show_stream_materialized_state(
    input: &str,
) -> error_stack::Result<ShowRelayMaterializedState, ParseFromSourceError> {
    let LexedInput {
        source,
        spanned_tokens,
        tokens,
    } = lex_input(input)?;
    parse_show_stream_materialized_state_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_show_stream_materialized_state(input: &str, cursor: usize) -> Vec<String> {
    suggest_from!(input, cursor, show_stream_materialized_state_parser())
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
    fn parses_show_stream_materialized_state() {
        let tokens = to_tokens("SHOW RELAY notifications MATERIALIZED STATE;");
        let parsed =
            parse_show_stream_materialized_state_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.relay.as_str(), "notifications");
    }

    #[test]
    fn suggests_relay_reference_after_show_stream() {
        let input = "SHOW RELAY ";
        let suggestions = suggest_show_stream_materialized_state(input, input.len());
        assert!(suggestions.contains(&"ref:relay".to_string()));
    }

    #[test]
    fn suggests_materialized_after_relay_reference() {
        let input = "SHOW RELAY notifications ";
        let suggestions = suggest_show_stream_materialized_state(input, input.len());
        assert!(suggestions.contains(&"MATERIALIZED".to_string()));
    }
}

#[cfg(test)]
mod inspection_clauses_tests {
    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_models::{InspectionFormat, StateBranchSelection, Statement};

    use crate::statement::{parse_statement, suggest_statement};

    #[test]
    fn selectors_limits_continuations_and_formats_are_semantic_models() {
        for selector in [
            "FOR ALL BRANCHES",
            "FOR UNBRANCHED",
            "FOR BRANCH VALUES { tenant = 'alpha', shard = -7 }",
        ] {
            let text = format!(
                "SHOW RELAY events MATERIALIZED STATE {selector} LIMIT 2 AFTER \
                 $cursor$opaque'position$cursor$ FORMAT JSON;"
            );
            let statement =
                parse_statement(&text).assured("the clauses are in their declared order");
            let Statement::ShowRelayMaterializedState(show) = &statement else {
                panic!("inspection");
            };
            assert_eq!(show.format, InspectionFormat::Json);
            assert!(show.selection.is_some());
            let page = show.page.as_ref().assured("a page was requested");
            assert_eq!(page.limit.get(), 2);
            assert_eq!(page.after.as_deref(), Some("opaque'position"));
            let canonical = statement
                .to_canonical_plexus()
                .assured("literal clauses render");
            assert_eq!(
                parse_statement(&canonical).assured("canonical grammar"),
                statement
            );
        }
        let Statement::ShowRelayMaterializedState(show) = parse_statement(
            "show relay events materialized state for unbranched limit 1 format text;",
        )
        .assured("keywords are case insensitive") else {
            panic!("inspection");
        };
        assert_eq!(show.selection, Some(StateBranchSelection::Unbranched));
        assert_eq!(show.format, InspectionFormat::Text);
    }

    #[test]
    fn invalid_page_and_selector_forms_are_refused() {
        for suffix in [
            "LIMIT 0",
            "LIMIT -1",
            "LIMIT 1.5",
            "LIMIT 18446744073709551616",
            "AFTER 'position'",
            "LIMIT 1 AFTER 3",
            "FOR BRANCH VALUES {}",
            "FOR BRANCH VALUES { tenant = 'a', tenant = 'b' }",
            "FOR BRANCH VALUES { tenant = null }",
            "FORMAT JSON LIMIT 1",
            "FOR UNBRANCHED FOR ALL BRANCHES",
            "FORMAT XML",
        ] {
            assert!(
                parse_statement(&format!("SHOW RELAY events MATERIALIZED STATE {suffix};"))
                    .is_err(),
                "{suffix}"
            );
        }
    }

    #[test]
    fn completion_composes_the_current_state_clauses_without_statement_leakage() {
        for (prefix, expected) in [
            ("SHOW RELAY events MATERIALIZED STATE FOR ", "ALL BRANCHES"),
            ("SHOW RELAY events MATERIALIZED STATE FOR ", "BRANCH VALUES"),
            ("SHOW RELAY events MATERIALIZED STATE FORMAT ", "JSON"),
            ("SHOW RELAY events MATERIALIZED STATE LIMIT 1 ", "AFTER"),
        ] {
            let suggestions = suggest_statement(prefix, prefix.len());
            assert!(
                suggestions.iter().any(|item| item == expected),
                "{prefix}: {suggestions:?}"
            );
            assert!(
                !suggestions
                    .iter()
                    .any(|item| item == "CREATE" || item == "RESET"),
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
            "FOR BRANCH VALUES { tenant = 'alpha', shard = -7 }",
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
            "FOR BRANCH VALUES {}",
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
                "BRANCH VALUES",
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
