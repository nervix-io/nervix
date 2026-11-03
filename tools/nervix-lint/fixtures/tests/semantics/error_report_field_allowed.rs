//! The whole chain, a debug rendering, other values and same-named unrelated items are distinct controls.
use nervix_lint_fixtures::{FixtureFailure, error_stack::Report, tracing};

mod unrelated {
    /// A value that only shares the report's name.
    pub struct Report;

    impl std::fmt::Display for Report {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("an unrelated value")
        }
    }

    /// A function that only shares the tracing field constructor's name.
    pub fn display<T: std::fmt::Display>(value: T) -> String {
        value.to_string()
    }
}

pub fn whole_chain(error: Report<FixtureFailure>) {
    tracing::warn!(error = %format_args!("{error:#}"), "the fixture operation failed");
}

pub fn debug(error: Report<FixtureFailure>) {
    tracing::warn!(error = ?error, "the fixture operation failed");
}

pub fn foreign(error: std::io::Error) {
    tracing::warn!(error = %error, "the fixture operation failed");
}

pub fn current_context(error: &Report<FixtureFailure>) {
    tracing::warn!(error = %error.current_context(), "the fixture operation failed");
}

pub fn same_named_type(report: unrelated::Report) {
    tracing::warn!(report = %report, "the fixture operation failed");
}

pub fn same_named_function(error: Report<FixtureFailure>) -> String {
    unrelated::display(&error)
}
