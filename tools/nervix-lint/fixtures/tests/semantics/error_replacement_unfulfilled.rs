//! Preserving the cause does not fulfill an expectation for dropping it.
use nervix_lint_fixtures::{FixtureFailure, error_stack::{Report, ResultExt as _}};

#[cfg_attr(nervix_lint, expect(nervix::replaced_error, reason = "this calibration requires an actual replacement diagnostic"))]
pub fn retained(failure: std::io::Result<()>) -> Result<(), Report<FixtureFailure>> {
    failure.change_context(FixtureFailure)
}
