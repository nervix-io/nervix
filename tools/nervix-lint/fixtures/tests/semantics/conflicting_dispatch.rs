//! Dispatch boundaries have one reason on the owning callable.
#[cfg_attr(nervix_lint, nervix::dispatch(reason = "driver boundary"))]
#[cfg_attr(nervix_lint, nervix::dispatch(reason = "callback boundary"))]
pub fn invoke(callback: fn()) { callback(); }
