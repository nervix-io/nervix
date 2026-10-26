//! Incomplete classification metadata cannot suppress a failure.
use nervix_lint_fixtures::FixtureFailure;
#[derive(Debug)]
#[cfg_attr(nervix_lint, nervix::error_boundary(outcome, reason = ""))]
pub struct Failure;
pub fn run() -> Result<(), Failure> { Err(Failure) }
pub fn other() -> Result<(), FixtureFailure> { Err(FixtureFailure) }
impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str("typed outcome") }
}
impl std::error::Error for Failure {}
