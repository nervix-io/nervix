//! An expectation must still match a diagnosed panic API.
pub fn run(value: Option<u32>) -> u32 {
    nervix_lint_fixtures::expect_lint!(nervix::bare_panic,
        "the API under qualification guarantees presence", value.unwrap_or(7))
}
