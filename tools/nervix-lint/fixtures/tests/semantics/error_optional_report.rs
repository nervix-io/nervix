//! A possibly absent report is not a complete failure carrier.
use nervix_lint_fixtures::{FixtureFailure, error_stack::Report};
#[derive(Debug)]
pub struct Failure { pub report: Option<Report<FixtureFailure>> }
impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str("failure") }
}
impl std::error::Error for Failure {}
pub fn run() -> Result<(), Failure> { Err(Failure { report: None }) }
