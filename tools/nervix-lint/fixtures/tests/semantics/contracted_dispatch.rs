//! Source-contract compiler fixture; no runtime behavior is asserted.

#[cfg_attr(nervix_lint, nervix::context(recurring, reason = "the caller executes this contract"))]
#[cfg_attr(nervix_lint, nervix::dispatch(reason = "the caller supplies a callback under this recurring contract"))]
pub fn batch(callback: fn()) { callback(); }
