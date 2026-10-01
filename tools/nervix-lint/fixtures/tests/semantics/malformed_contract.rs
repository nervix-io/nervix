//! Source-contract compiler fixture; no runtime behavior is asserted.

#[cfg_attr(nervix_lint, nervix::context(bounded, reason = "protocol has no declared bound"))]
pub fn resolve() {}
