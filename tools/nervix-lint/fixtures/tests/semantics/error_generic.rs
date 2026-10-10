//! Generic substitution reveals the concrete owning failure type.
use nervix_lint_fixtures::FixtureFailure;
fn identity<E>(error: E) -> Result<(), E> { Err(error) }
pub fn run() { assert!(identity(FixtureFailure).is_err()); }
