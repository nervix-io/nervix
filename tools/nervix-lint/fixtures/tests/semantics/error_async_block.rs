//! An inferred async block still exposes its failure output.
use nervix_lint_fixtures::FixtureFailure;
pub async fn run() {
    let operation = async { Err::<(), _>(FixtureFailure) };
    assert!(operation.await.is_err());
}
