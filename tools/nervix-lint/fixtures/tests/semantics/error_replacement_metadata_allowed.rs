//! The caller consumes the producer's metadata without inheriting a surrounding annotation.
use nervix_lint_fixtures::{FixtureFailure, error_stack::Report};

pub fn type_contract(answer: Result<(), callee::Refusal>) -> Result<(), Report<FixtureFailure>> {
    answer.map_err(|_| Report::new(FixtureFailure))
}

pub fn return_contract() -> Result<(), Report<FixtureFailure>> {
    let answer = callee::refusal();
    answer.map_err(|_| Report::new(FixtureFailure))
}
