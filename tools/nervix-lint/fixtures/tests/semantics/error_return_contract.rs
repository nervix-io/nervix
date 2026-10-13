//! An exact return classification applies to that callable alone.
use nervix_lint_fixtures::FixtureFailure;
#[cfg_attr(nervix_lint, nervix::error_boundary(outcome, reason = "the conversion returns its ordinary refusal without executing work"))]
pub fn convert() -> Result<(), FixtureFailure> { Err(FixtureFailure) }
