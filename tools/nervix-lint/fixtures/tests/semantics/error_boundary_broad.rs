//! Module-wide failure classifications are not contracts.
#[cfg_attr(nervix_lint, nervix::error_boundary(outcome, reason = "all errors are ordinary"))]
pub mod operations {}
