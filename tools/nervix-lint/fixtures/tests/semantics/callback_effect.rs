//! Source-contract compiler fixture; no runtime behavior is asserted.

#[cfg_attr(nervix_lint, nervix::context(recurring, reason = "the caller executes this contract"))]
pub fn batch(map: &nervix_lint_fixtures::MapAlias) { let callback = || drop(map.get(&1)); callback(); }
