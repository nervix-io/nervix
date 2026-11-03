//! A tracing field that records a report by its plain Display is refused at its authored value.
use nervix_lint_fixtures::{FixtureFailure, SharedValue, error_stack::Report, tracing};

type Failure = Report<FixtureFailure>;

macro_rules! report_failure {
    ($failure:expr) => {
        tracing::warn!(error = %$failure, "the fixture operation failed")
    };
}

macro_rules! report_current {
    () => {{
        let current = Report::new(FixtureFailure);
        tracing::warn!(error = %current, "the fixture operation failed");
    }};
}

pub fn assigned(error: Report<FixtureFailure>) {
    tracing::warn!(error = %error, "the fixture operation failed");
}

pub fn shorthand(failure: &Failure) {
    tracing::warn!(%failure, "the fixture operation failed");
}

pub fn boxed(boxed: Box<Report<FixtureFailure>>) {
    tracing::error!(error = tracing::field::display(&boxed), "the fixture operation failed");
}

pub fn shared(shared: SharedValue<Report<FixtureFailure>>) {
    use tracing::field::display as rendered;
    tracing::info!(error = rendered(&shared), "the fixture operation failed");
}

pub fn wrapped(wrapped: Report<FixtureFailure>) {
    report_failure!(wrapped);
}

pub fn internal() {
    report_current!();
}

pub struct Holder {
    pub held: Report<FixtureFailure>,
}

impl Holder {
    pub fn field(&self) {
        tracing::warn!(%self.held, "the fixture operation failed");
    }
}
