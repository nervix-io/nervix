//! A checked operation cannot retain an exception for clamped arithmetic.
pub fn checked(value: u64) -> Option<u64> {
    nervix_lint_fixtures::expect_lint!(
        nervix::clamped_arithmetic,
        "the fixture declares modular arithmetic",
        value.checked_mul(31)
    )
}

pub fn projection(value: Option<u32>) -> Option<u32> {
    nervix_lint_fixtures::expect_lint!(
        nervix::combinator_control_flow,
        "the fixture declares a canonical fallback decision",
        value.map(|item| item)
    )
}
