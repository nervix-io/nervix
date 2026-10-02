//! A value parameter does not own an execution context.
pub fn batch(#[cfg_attr(nervix_lint, nervix::context(lifecycle, reason = "parameter value"))] _value: u32) {}
