//! A reviewed panic is scoped to exactly one resolved operation.
pub fn run(value: Option<u32>) -> u32 {
    nervix_lint_fixtures::expect_lint!(nervix::bare_panic,
        "the API under qualification guarantees presence", value.unwrap())
}
