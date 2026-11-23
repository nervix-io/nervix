//! An expectation on a value that is already fixed or derived hides nothing.
#[cfg_attr(nervix_lint, nervix::constant(fixed, reason = "the frame ABI stores its tag in four bytes"))]
#[cfg_attr(nervix_lint, expect(nervix::hidden_tunable, reason = "the fixture expects a debt the constant does not have"))]
pub const TAG_BYTES: usize = 4;

pub fn derived(header: &[u8]) -> Vec<u8> {
    nervix_lint_fixtures::expect_lint!(
        nervix::hidden_tunable,
        "the fixture expects a literal the call does not write",
        Vec::with_capacity(header.len())
    )
}
