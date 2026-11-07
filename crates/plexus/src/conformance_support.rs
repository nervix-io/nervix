//! What the conformance tests generated from connector declarations share.
//!
//! Layer: test harness.
//!
//! - **Owns.** Parsing a generated statement through the composed statement grammar, the check
//!   that canonical Plexus writes a parsed statement back as itself, and the check that
//!   completion offers exactly the items a declaration lists.
//! - **Depends on.** The composed statement grammar and its completion.
//! - **Must not know.** Any one connector, entity or grammar module.
//!
//! The generated tests go through the composed grammar, as a REPL or a client does, so a branch of
//! another statement family that leaks into an entity's position fails them too.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{Model, RequestedResourceVersion, Statement};

use crate::statement::{parse_statement, suggest_statement};

/// Checks the shared input acknowledgement clause through a family's production grammar.
pub(crate) fn assert_input_acknowledgement_clause(prefix: &str, tail: &str) {
    for collection in ["", " COLLECT FOR 10ms MAX BATCH SIZE 1MiB"] {
        let input = format!("{prefix}{collection}");
        let implicit = parse_statement(&format!("{input} {tail};"))
            .assured("each family supplies a complete statement with its optional mode omitted");
        for mode in ["ATTACHED", "DETACHED"] {
            let source = format!("{input} {mode} {tail};");
            let explicit =
                parse_statement(&source).unwrap_or_else(|error| panic!("{source}\n{error:?}"));
            if mode == "ATTACHED" {
                assert_eq!(implicit, explicit, "omitting the clause means ATTACHED");
            } else {
                assert_ne!(implicit, explicit, "DETACHED is a distinct input contract");
            }
            let canonical = explicit
                .to_canonical_plexus()
                .assured("these fixtures contain only canonically representable expressions");
            let lines: Vec<_> = canonical.lines().map(str::trim).collect();
            assert!(
                lines.windows(2).any(|pair| {
                    (pair[0].starts_with("FROM ") || pair[0].starts_with("RIGHT FROM "))
                        && pair[1] == mode
                }),
                "{canonical}\nmust write {mode} after the complete input contract"
            );
            assert_reads_back_canonically(&explicit);
        }
        assert!(refused(&format!("{input} ATTACHED DETACHED {tail};")));
        assert!(refused(&format!("{input} DETACHED ATTACHED {tail};")));
        let completion_prefix = format!("{input} ");
        let suggestions = suggest_statement(&completion_prefix, completion_prefix.len());
        for mode in ["ATTACHED", "DETACHED"] {
            assert!(
                suggestions.iter().any(|item| item == mode),
                "{suggestions:?}"
            );
        }
    }
    let boundary = prefix
        .find(" LEFT FROM ")
        .or_else(|| prefix.find(" FROM "))
        .assured("every fixture declares relay inputs");
    let (head, inputs) = prefix.split_at(boundary);
    assert!(refused(&format!("{head} DETACHED{inputs} {tail};")));
}

/// The Model a generated `CREATE` statement declares, or `None` where the grammar refuses it.
pub(crate) fn created_model(statement: &str) -> Option<Model<RequestedResourceVersion>> {
    let Ok(Statement::Create(create)) = parse_statement(statement) else {
        return None;
    };
    Some(*create.body)
}

/// Whether the grammar refuses `statement`.
pub(crate) fn refused(statement: &str) -> bool {
    parse_statement(statement).is_err()
}

/// Checks that canonical Plexus writes `statement` as text the composed grammar reads back as the
/// same statement, so every operation a declaration generates renders as the operation it is.
pub(crate) fn assert_reads_back_canonically(statement: &Statement) {
    let canonical = match statement.to_canonical_plexus() {
        Ok(canonical) => canonical,
        Err(error) => panic!("{statement:?} must render as canonical Plexus: {error:?}"),
    };
    let reparsed = match parse_statement(&canonical) {
        Ok(reparsed) => reparsed,
        Err(error) => panic!("{canonical}\nmust parse again: {error:?}"),
    };
    assert_eq!(
        &reparsed, statement,
        "{canonical}\nchanged when it was parsed again"
    );
}

/// Whether `unit` opens `text`: the whole text is the unit, or the text goes on after it with a
/// space.
fn opens(text: &str, unit: &str) -> bool {
    match text.strip_prefix(unit) {
        Some(rest) => rest.is_empty() || rest.starts_with(' '),
        None => false,
    }
}

/// Whether a suggestion is punctuation that goes on with what was written last, such as the `-`
/// that joins the words of an unquoted queue name. A declaration lists words and labels, never
/// punctuation, so such a suggestion is the grammar of the value before it rather than an item
/// a declaration could list.
fn continues_what_was_written(suggestion: &str) -> bool {
    !suggestion.chars().any(char::is_alphanumeric)
}

/// Checks that completion at the end of `prefix` offers exactly the items `continuations` begin
/// with: every suggestion opens one of the continuations, so nothing another entity or another
/// statement declares leaks into the position, and every continuation opens with one of the
/// suggestions, so nothing declared is missing from it.
pub(crate) fn assert_offers_exactly(prefix: &str, continuations: &[&str]) {
    let suggestions = suggest_statement(prefix, prefix.len());
    for suggestion in &suggestions {
        if continues_what_was_written(suggestion) {
            continue;
        }
        let declared = continuations
            .iter()
            .any(|continuation| opens(continuation, suggestion));
        assert!(
            declared,
            "completion of `{prefix}` offers `{suggestion}`, which opens none of \
             {continuations:?}; it offered {suggestions:?}"
        );
    }
    for continuation in continuations {
        let offered = suggestions
            .iter()
            .any(|suggestion| opens(continuation, suggestion));
        assert!(
            offered,
            "completion of `{prefix}` offers nothing that opens `{continuation}`; it offered \
             {suggestions:?}"
        );
    }
}
