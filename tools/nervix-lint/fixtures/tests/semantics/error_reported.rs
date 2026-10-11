//! Resolved error-stack aliases and complete report carriers satisfy the contract.
use nervix_lint_fixtures::{error_stack::{self, Report}, FixtureFailure};
pub type Contextual<T> = error_stack::Result<T, FixtureFailure>;
pub fn run() -> Contextual<()> { Err(Report::new(FixtureFailure)) }
#[derive(Debug)]
pub struct Carrier(Box<Report<FixtureFailure>>);
impl std::fmt::Display for Carrier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { self.0.fmt(f) }
}
impl std::error::Error for Carrier {}
pub trait Hook { fn request(&self) -> Result<(), Carrier>; }
pub struct Service;
impl Hook for Service {
    fn request(&self) -> Result<(), Carrier> { Err(Carrier(Box::new(Report::new(FixtureFailure)))) }
}
