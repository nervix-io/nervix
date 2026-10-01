//! Trait metadata supplies a recurring effect contract to dynamic callers.
#[cfg_attr(nervix_lint, nervix::context(recurring, reason = "batch callback"))]
pub trait Callback { fn run(&self); }
#[cfg_attr(nervix_lint, nervix::context(recurring, reason = "batch callback"))]
pub fn batch(callback: &dyn Callback) { callback.run(); }
