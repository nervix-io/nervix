//! One expectation cannot hide two unrelated arithmetic operations.
pub fn two(value: u64) -> u64 {
    #[cfg_attr(nervix_lint, expect(nervix::clamped_arithmetic, reason = "bounded mixer"))]
    let mixed = value.wrapping_add(value.wrapping_mul(31));
    mixed
}

pub fn decisions(value: Option<u32>) -> u32 {
    #[cfg_attr(nervix_lint, expect(nervix::combinator_control_flow, reason = "canonical fallback"))]
    let answer = value.map_or(0, |item| item) + value.map_or(1, |item| item);
    answer
}
