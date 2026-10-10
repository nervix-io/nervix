//! A future alias still carries its concrete unreported error contract.
use nervix_lint_fixtures::{FixtureFailure, futures_core::future::BoxFuture};
type Outcome<T> = Result<T, FixtureFailure>;
pub fn operation() -> BoxFuture<'static, Outcome<()>> {
    Box::pin(async { Err(FixtureFailure) })
}
