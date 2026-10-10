//! An async closure exposes its coroutine output through inference.
use nervix_lint_fixtures::FixtureFailure;
pub async fn run() {
    let operation = async || Err::<(), _>(FixtureFailure);
    assert!(operation().await.is_err());
}
