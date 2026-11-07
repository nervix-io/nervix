//! Recording the whole chain does not fulfill an expectation for recording the outermost context.
use nervix_lint_fixtures::{FixtureFailure, error_stack::Report, tracing};

#[cfg_attr(nervix_lint, expect(nervix::partial_report_field, reason = "this calibration requires an actual partial report field"))]
pub fn whole_chain(error: Report<FixtureFailure>) {
    tracing::warn!(error = %format_args!("{error:#}"), "the fixture operation failed");
}
