//! One expectation cannot cover two independent informative-error replacements.
use nervix_lint_fixtures::{FixtureFailure, error_stack::Report};

#[cfg_attr(nervix_lint, expect(nervix::replaced_error, reason = "this deliberately broad calibration must be refused"))]
pub fn wide(first: std::io::Result<()>, second: std::io::Result<()>) -> Result<(), Report<FixtureFailure>> {
    first.map_err(|_| Report::new(FixtureFailure))?;
    second.map_err(|_| Report::new(FixtureFailure))
}
