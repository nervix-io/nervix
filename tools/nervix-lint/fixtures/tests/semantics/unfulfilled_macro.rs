//! Removing the acquisition from an operation macro leaves an unfulfilled expectation.
#[cfg_attr(nervix_lint, nervix::context(recurring, reason = "batch rows"))]
pub fn batch() {
    let value = nervix_lint_fixtures::expect_lint!(nervix::sync_acquisition, "one reviewed operation", 1_u32);
    assert_eq!(value, 1);
}
