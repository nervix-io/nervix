//! Each exceptional arithmetic operation holds its actual bounded algorithmic meaning.
pub fn mix(value: u64) -> u64 {
    nervix_lint_fixtures::expect_lint!(
        nervix::clamped_arithmetic,
        "the fixture is a modular hash mixer",
        value.wrapping_mul(31)
    )
}

pub fn fallback(value: Option<u32>) -> u32 {
    nervix_lint_fixtures::expect_lint!(
        nervix::combinator_control_flow,
        "this annotation fixture exercises one canonical fallback decision",
        value.map_or(0, |item| item)
    )
}
