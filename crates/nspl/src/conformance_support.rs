//! What the conformance tests generated from connector declarations share.
//!
//! Layer: test harness.
//!
//! - **Owns.** Parsing a generated statement through the composed statement grammar, and the
//!   check that completion offers exactly the items a declaration lists.
//! - **Depends on.** The composed statement grammar and its completion.
//! - **Must not know.** Any one connector, entity or grammar module.
//!
//! The generated tests go through the composed grammar, as a REPL or a client does, so a branch of
//! another statement family that leaks into an entity's position fails them too.

use nervix_models::{Model, RequestedResourceVersion, Statement};

use crate::statement::{parse_statement, suggest_statement};

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
