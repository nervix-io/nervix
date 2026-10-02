//! Source-contract compiler fixture; no runtime behavior is asserted.

#[cfg_attr(nervix_lint, nervix::context(recurring, reason = "the caller executes this contract"))]
pub fn batch(map: &nervix_lint_fixtures::MapAlias) {
#[cfg_attr(nervix_lint, expect(nervix::sync_acquisition))]
let value = map.get(&1); drop(value);
}
