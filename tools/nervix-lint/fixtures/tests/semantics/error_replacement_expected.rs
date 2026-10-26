//! A one-operation calibration expectation remains narrow and fulfilled.
use nervix_lint_fixtures::{FixtureFailure, error_stack::Report};

pub fn expected(failure: std::io::Result<()>) -> Result<(), Report<FixtureFailure>> {
    #[cfg_attr(nervix_lint, expect(nervix::replaced_error, reason = "this exact operation calibrates a reviewed replacement"))]
    failure.map_err(|_| Report::new(FixtureFailure))
}
