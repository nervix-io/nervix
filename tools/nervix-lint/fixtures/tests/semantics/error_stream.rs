//! A boxed stream exposes its resolved Item contract.
use std::pin::Pin;
use nervix_lint_fixtures::{FixtureFailure, futures_core::Stream};
type Items = Result<(), FixtureFailure>;
pub fn events() -> Pin<Box<dyn Stream<Item = Items>>> { loop {} }
