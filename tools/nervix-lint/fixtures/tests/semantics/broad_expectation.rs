//! Source-contract compiler fixture; no runtime behavior is asserted.

#[cfg_attr(nervix_lint, nervix::context(recurring, reason = "the caller executes this contract"))]
#[cfg_attr(nervix_lint, expect(nervix::sync_acquisition, reason = "fixture owner retains this single reviewed operation"))]
pub fn batch(map: &nervix_lint_fixtures::MapAlias) { drop(map.get(&1)); }
