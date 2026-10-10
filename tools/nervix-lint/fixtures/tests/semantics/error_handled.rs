//! Ordinary values and explicit recovery preserve their chosen outcome.
use nervix_lint_fixtures::{meticulous::ResultExt as _, nervix_recovery::Discarded as _};
pub fn run(first: std::io::Result<()>, second: std::io::Result<()>, guaranteed: Result<u32, ()>) {
    let _ = (5_u32, Some(8_u32));
    let _ = &first;
    first.discarded("the caller already recorded this attempt");
    if let Err(_) = second { return; }
    let value = guaranteed.assured("construction has already validated this value");
    assert!(value > 0);
}
