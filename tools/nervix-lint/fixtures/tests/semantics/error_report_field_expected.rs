//! A reviewed field that records a report's outermost context is scoped to one resolved operation.
use nervix_lint_fixtures::{FixtureFailure, error_stack::Report, tracing};

pub fn reviewed(error: Report<FixtureFailure>) {
    nervix_lint_fixtures::expect_lint!(nervix::partial_report_field,
        "the calibration records the outermost context alone",
        tracing::warn!(error = %error, "the fixture operation failed"));
}
