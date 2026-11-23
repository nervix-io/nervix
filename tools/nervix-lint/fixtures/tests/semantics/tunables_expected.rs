//! A retained value states, on that one operation, why it stays outside a setting.
#[cfg_attr(nervix_lint, expect(nervix::hidden_tunable, reason = "the fixture retains one constant until its repair task https://app.clickup.com/t/86bck6jqp"))]
pub const RETAINED: usize = 4;

pub fn reserved() -> Vec<u8> {
    nervix_lint_fixtures::expect_lint!(
        nervix::hidden_tunable,
        "the fixture retains one capacity until its repair task",
        Vec::with_capacity(16)
    )
}
