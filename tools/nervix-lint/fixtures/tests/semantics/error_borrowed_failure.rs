//! An alias borrowing an owning failure definition still needs contextual reporting.
use nervix_lint_fixtures::FixtureFailure;
pub type FailureRef<'a> = &'a FixtureFailure;
pub fn run(error: FailureRef<'_>) -> Result<(), FailureRef<'_>> { Err(error) }
