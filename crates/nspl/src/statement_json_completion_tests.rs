//! Completion around the JSON extraction forms of a route expression.
//!
//! Layer: test harness.
//!
//! - **Owns.** Proving that a complete or unfinished JSON extraction leaves route completion where
//!   any other expression leaves it.
//! - **Depends on.** Statement completion.
//! - **Must not know.** How an extraction is parsed into a Model.

use super::suggest_statement;

#[test]
fn json_extraction_body_does_not_change_route_completion_context() {
    let literal = "CREATE JUNCTION merge FROM orders UNBRANCHED TO routed SET result = 1 ";
    for expression in [
        "JSON_VALUE(input.doc, '$.count' AS I64)",
        "TRY_JSON_VALUE(input.doc, '$[\"odd key\"][0]' AS VEC<VEC<I64>>)",
        "JSON_VALUE(input.doc, '$.point' AS ARRAY<F64, 2>)",
        "JSON_EXISTS(input.doc, '$.note')",
    ] {
        let input = format!(
            "CREATE JUNCTION merge FROM orders UNBRANCHED TO routed SET result = {expression} "
        );
        assert_eq!(
            suggest_statement(literal, literal.len()),
            suggest_statement(&input, input.len()),
            "completion changed after {expression}",
        );
    }

    let unfinished_conditional =
        "CREATE JUNCTION merge FROM orders UNBRANCHED TO routed SET result = CASE WHEN ";
    for unfinished in [
        "JSON_VALUE(input.doc, '$.a' AS ",
        "JSON_VALUE(input.doc, '$.a' AS VEC<",
        "JSON_EXISTS(input.doc, ",
    ] {
        let input = format!(
            "CREATE JUNCTION merge FROM orders UNBRANCHED TO routed SET result = {unfinished}"
        );
        assert_eq!(
            suggest_statement(unfinished_conditional, unfinished_conditional.len()),
            suggest_statement(&input, input.len()),
            "an unfinished extraction offers what any unfinished expression offers: {unfinished}"
        );
    }
}
