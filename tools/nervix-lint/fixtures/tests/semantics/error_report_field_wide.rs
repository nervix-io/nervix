//! One expectation cannot cover two independent partial report fields.
use nervix_lint_fixtures::{FixtureFailure, error_stack::Report, tracing};

#[cfg_attr(nervix_lint, expect(nervix::partial_report_field, reason = "this deliberately broad calibration must be refused"))]
pub fn wide(first: Report<FixtureFailure>, second: Report<FixtureFailure>) {
    tracing::warn!(error = %first, "the first fixture operation failed");
    tracing::warn!(error = %second, "the second fixture operation failed");
}
