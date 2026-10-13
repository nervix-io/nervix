//! Boxing a concrete local error does not create a contextual report.
use nervix_lint_fixtures::FixtureFailure;
pub fn run() -> Result<(), Box<FixtureFailure>> { Err(Box::new(FixtureFailure)) }
