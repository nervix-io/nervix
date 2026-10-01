//! Source-contract compiler fixture; no runtime behavior is asserted.

pub struct State {
#[cfg_attr(nervix_lint, nervix::context(lifecycle, reason = "the caller executes this contract"))]
pub value: u32,
}
