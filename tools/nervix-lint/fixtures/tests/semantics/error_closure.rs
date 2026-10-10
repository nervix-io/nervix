//! Inference does not hide a closure's failure contract.
use nervix_lint_fixtures::FixtureFailure;
pub fn run() {
    let operation = || Err::<(), _>(FixtureFailure);
    assert!(operation().is_err());
}
