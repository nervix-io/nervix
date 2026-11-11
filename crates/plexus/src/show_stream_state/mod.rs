use chumsky::prelude::*;
use meticulous::OptionExt as _;
use nervix_models::ShowRelayMaterializedState;

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        LexedInput, ParseError, ParseFromSourceError, into_parse_error, kw, lex_input, relay_ref,
        suggest_from,
    },
    state_inspection_clauses::state_inspection_clauses,
};

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
            "FOR BRANCH KEY { tenant = 'alpha', shard = -7 }",
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
            "FOR BRANCH KEY {}",
            "FOR BRANCH KEY { tenant = 'a', tenant = 'b' }",
            "FOR BRANCH KEY { tenant = null }",
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
            ("SHOW RELAY events MATERIALIZED STATE FOR ", "BRANCH KEY"),
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
