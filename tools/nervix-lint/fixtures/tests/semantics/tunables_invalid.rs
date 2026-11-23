//! An exception names its reason.
#[cfg_attr(nervix_lint, expect(nervix::hidden_tunable))]
pub const UNEXPLAINED: usize = 4;

pub fn blank() -> Vec<u8> {
    #[cfg_attr(nervix_lint, expect(nervix::hidden_tunable, reason = " "))]
    let buffer = Vec::with_capacity(8);
    buffer
}
