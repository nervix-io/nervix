//! Both API rules reject an expectation with no meaningful source-owned reason.
pub fn arithmetic(value: u32) -> u32 {
    #[cfg_attr(nervix_lint, expect(nervix::clamped_arithmetic))]
    let answer = value.wrapping_add(1);
    answer
}
pub fn decision(value: Option<u32>) -> u32 {
    #[cfg_attr(nervix_lint, expect(nervix::combinator_control_flow, reason = " "))]
    let answer = value.map_or(0, |item| item);
    answer
}
