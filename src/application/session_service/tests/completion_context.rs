//! The completion context a Suggest request's grammar input and prefix are read from.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** Unit tests of the completion context: the grammar input, cursor and prefix it keeps,
//!   the keyword and phrase ranges it matches against the original input, and the resource a
//!   rebind member names across whitespace.
//! - **Depends on.** The session service's completion context and the grammar its thread holds.
//! - **Must not know.** Sessions, transactions or the models a completion offers.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        harness,
        reason = "completion context unit tests remain outside product execution"
    )
)]

use nervix_models::ResourceName;

use super::super::{
    CompletionContext, CompletionExpectation, completion_context, rebind_resource_before_for,
    with_statement_grammar, word_end,
};

#[test]
fn completion_context_preserves_prefix_for_post_filtering() {
    let input = "CREATE SCHE";
    let CompletionContext {
        grammar_input,
        grammar_cursor,
        prefix,
        ..
    } = completion_context(input, input.len());

    assert_eq!(grammar_input, "CREATE ");
    assert_eq!(grammar_cursor, "CREATE ".len());
    assert_eq!(prefix, "sche");

    let input = "SHOW CLUSTR;";
    let CompletionContext {
        grammar_input,
        grammar_cursor,
        prefix,
        ..
    } = completion_context(input, "SHOW CLU".len());
    assert_eq!(grammar_input, "SHOW ;");
    assert_eq!(grammar_cursor, "SHOW ".len());
    assert_eq!(prefix, "clu");
    assert_eq!(word_end(input, "SHOW CLU".len()), "SHOW CLUSTR".len());
}

#[test]
fn keyword_completion_is_filtered_by_original_prefix() {
    let input = "CREATE SCHE";
    let context = completion_context(input, input.len());
    let expectations = with_statement_grammar(|grammar| {
        grammar.suggest_expectations(&context.grammar_input, context.grammar_cursor)
    });
    let filtered = expectations
        .into_iter()
        .filter_map(|item| {
            let CompletionExpectation::Literal(item) = item else {
                return None;
            };
            context.literal_range(input, &item).map(|_| item)
        })
        .collect::<Vec<_>>();

    assert_eq!(filtered, vec!["SCHEMA".to_string()]);
}

#[test]
fn phrase_completion_replaces_matching_words_around_the_cursor() {
    let input = "TIME RA";
    let context = completion_context(input, input.len());
    assert_eq!(
        context.literal_range(input, "TIME RATE"),
        Some(0..input.len())
    );

    let input = "TIME RATE;";
    let context = completion_context(input, 2);
    assert_eq!(context.literal_range(input, "TIME RATE"), Some(0..9));

    let input = "OTHER RA";
    let context = completion_context(input, input.len());
    assert_eq!(context.literal_range(input, "TIME RATE"), None);
}

#[test]
fn rebind_member_completion_finds_the_resource_across_whitespace() {
    for input in [
        "REBIND RESOURCE bundle TO VERSION 2 FOR HASH MAP ",
        "REBIND\nRESOURCE\tbundle\nTO\tVERSION 2\nFOR\tHASH MAP ",
    ] {
        assert_eq!(
            rebind_resource_before_for(input, input.len())
                .as_ref()
                .map(ResourceName::as_str),
            Some("bundle")
        );
    }
    assert_eq!(
        rebind_resource_before_for("REBIND RESOURCE bundle TO VERSION 2 ", 36),
        None
    );
}
