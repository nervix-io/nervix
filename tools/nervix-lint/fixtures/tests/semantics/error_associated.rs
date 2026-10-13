//! Associated return contracts normalize before failure classification.
use nervix_lint_fixtures::FixtureFailure;
pub trait Contract { type Output; }
pub struct Operation;
impl Contract for Operation { type Output = Result<(), FixtureFailure>; }
pub fn run() -> <Operation as Contract>::Output { Err(FixtureFailure) }
