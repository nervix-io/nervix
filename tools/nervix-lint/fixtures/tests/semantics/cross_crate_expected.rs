//! Source-contract compiler fixture; no runtime behavior is asserted.

#[cfg_attr(nervix_lint, nervix::context(recurring, reason = "the caller executes this contract"))]
pub fn batch() {
#[cfg_attr(nervix_lint, expect(nervix::lifecycle_call, reason = "fixture owner documents this one exceptional installation"))]
callee::install();
}
