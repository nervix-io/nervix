//! Source-contract compiler fixture; no runtime behavior is asserted.

#[cfg_attr(nervix_lint, nervix::context(recurring, reason = "the caller executes this contract"))]
#[cfg_attr(nervix_lint, nervix::context(lifecycle, reason = "the caller executes this contract"))]
pub fn work() {}
