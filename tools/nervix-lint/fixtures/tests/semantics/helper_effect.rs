//! Source-contract compiler fixture; no runtime behavior is asserted.

fn helper(map: &nervix_lint_fixtures::MapAlias) { drop(map.get(&1)); }
#[cfg_attr(nervix_lint, nervix::context(recurring, reason = "the caller executes this contract"))]
pub fn batch(map: &nervix_lint_fixtures::MapAlias) { helper(map); }
