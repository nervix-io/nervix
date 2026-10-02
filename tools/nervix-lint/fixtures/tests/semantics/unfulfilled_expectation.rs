//! Source-contract compiler fixture; no runtime behavior is asserted.

#[cfg_attr(nervix_lint, nervix::context(recurring, reason = "the caller executes this contract"))]
pub fn batch() {
#[cfg_attr(nervix_lint, expect(nervix::sync_acquisition, reason = "fixture owner retains this single reviewed operation"))]
let value = 1; core::hint::black_box(value);
}
