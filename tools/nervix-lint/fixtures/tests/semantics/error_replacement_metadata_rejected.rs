//! An informative producer is not classified by another function in its crate.
use nervix_lint_fixtures::{FixtureFailure, error_stack::Report};

pub fn informative() -> Result<(), Report<FixtureFailure>> {
    callee::informative().map_err(|_| Report::new(FixtureFailure))
}
