//! Informative foreign failures and nested reports must survive a new failure context.
#![cfg_attr(nervix_lint, nervix::context(lifecycle, reason = "compiler calibration exercises each replacement once"))]

use nervix_lint_fixtures::{FixtureFailure, error_stack::Report};

type IoResult<T> = std::io::Result<T>;
type FailureReport = Report<FixtureFailure>;

pub fn foreign(failure: IoResult<()>) -> Result<(), FailureReport> {
    failure.map_err(|_| Report::new(FixtureFailure))
}

pub fn nested(failure: Result<(), FailureReport>) -> Result<(), FailureReport> {
    failure.map_err(|_| Report::new(FixtureFailure))
}

pub fn explicit_return(failure: IoResult<()>) -> Result<(), FailureReport> {
    failure.map_err(|_| return Report::new(FixtureFailure))
}

pub fn alternative(failure: IoResult<()>) -> Result<(), FailureReport> {
    failure.or_else(|_| Err(Report::new(FixtureFailure)))
}

pub fn fallback(failure: IoResult<FailureReport>) -> FailureReport {
    failure.unwrap_or_else(|_| Report::new(FixtureFailure))
}

pub fn matched(failure: IoResult<()>) -> Result<(), FailureReport> {
    match failure {
        Ok(value) => Ok(value),
        Err(_) => Err(Report::new(FixtureFailure)),
    }
}

pub fn conditional(failure: IoResult<()>) -> Result<(), FailureReport> {
    let Ok(value) = failure else {
        return Err(Report::new(FixtureFailure));
    };
    Ok(value)
}

pub fn matched_foreign_error(failure: IoResult<()>) -> std::io::Error {
    match failure {
        Ok(()) => std::io::Error::other("unexpected success"),
        Err(_) => std::io::Error::other("replacement failure"),
    }
}

pub fn fallback_foreign_error(failure: IoResult<std::io::Error>) -> std::io::Error {
    failure.unwrap_or_else(|_| std::io::Error::other("replacement failure"))
}

#[cfg_attr(nervix_lint, nervix::error_boundary(outcome, reason = "the enclosing return contract must not classify a different operation"))]
pub fn enclosing(failure: IoResult<()>) -> Result<(), FailureReport> {
    failure.map_err(|_| Report::new(FixtureFailure))
}

macro_rules! replace_failure {
    ($failure:expr) => {
        $failure.map_err(|_| Report::new(FixtureFailure))
    };
}

pub fn authored_macro(failure: IoResult<()>) -> Result<(), FailureReport> {
    replace_failure!(failure)
}

#[derive(Debug)]
#[cfg_attr(nervix_lint, nervix::error_boundary(library, reason = "an external signature classification does not make its informative error an outcome"))]
pub struct TryFromIntError(pub u8);
impl std::fmt::Display for TryFromIntError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "operation failure {}", self.0)
    }
}
impl std::error::Error for TryFromIntError {}

pub fn merely_named(failure: Result<(), TryFromIntError>) -> Result<(), FailureReport> {
    failure.map_err(|_| Report::new(FixtureFailure))
}

pub struct FieldLookup;
impl FieldLookup {
    pub fn index_of(&self, _field: &str) -> std::io::Result<usize> {
        Err(std::io::Error::other("lookup operation failed"))
    }
}

pub fn merely_named_lookup(value: &FieldLookup) -> Result<usize, FailureReport> {
    value.index_of("field").map_err(|_| Report::new(FixtureFailure))
}

pub fn bound_callback(failure: IoResult<()>) -> Result<(), FailureReport> {
    let replace = |_: std::io::Error| Report::new(FixtureFailure);
    failure.map_err(replace)
}

#[cfg_attr(nervix_lint, nervix::error_boundary(outcome, reason = "the producer exposes an ordinary typed refusal"))]
pub fn declared_outcome() -> IoResult<()> {
    Err(std::io::Error::other("refusal"))
}

pub fn changed_failure(failure: IoResult<()>) -> Result<(), FailureReport> {
    let changed = declared_outcome().and_then(|()| failure);
    changed.map_err(|_| Report::new(FixtureFailure))
}

pub fn reassigned(failure: IoResult<()>) -> Result<(), FailureReport> {
    let mut answer = declared_outcome();
    std::hint::black_box(&answer);
    answer = failure;
    answer.map_err(|_| Report::new(FixtureFailure))
}
