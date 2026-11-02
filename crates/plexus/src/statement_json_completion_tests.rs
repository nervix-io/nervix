//! Completion around the JSON extraction forms of a route expression.
//!
//! Layer: test harness.
//!
//! - **Owns.** Proving that a complete or unfinished JSON extraction leaves route completion where
//!   any other expression leaves it.
//! - **Depends on.** Statement completion.
//! - **Must not know.** How an extraction is parsed into a Model.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

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

    for unfinished in [
        "JSON_VALUE(input.doc, '$.a' AS ",
        "JSON_VALUE(input.doc, '$.a' AS VEC<",
        "JSON_EXISTS(input.doc, ",
    ] {
        let input = format!(
            "CREATE JUNCTION merge FROM orders UNBRANCHED TO routed SET result = {unfinished}"
        );
        assert_eq!(
            suggest_statement(&input, input.len()),
            Vec::<String>::new(),
            "a type or JSON-path literal position is not an expression operand: {unfinished}"
        );
    }
    let operand =
        "CREATE JUNCTION merge FROM orders UNBRANCHED TO routed SET result = JSON_EXISTS(";
    assert!(
        suggest_statement(operand, operand.len()).contains(&"ref:expression_function".to_string())
    );
}
