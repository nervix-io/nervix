//! Source-contract compiler fixture; no runtime behavior is asserted.

#[cfg_attr(nervix_lint, nervix::context(lifecycle, reason = "the caller executes this contract"))]
pub fn install(map: &nervix_lint_fixtures::MapAlias) { drop(map.get(&1)); }
