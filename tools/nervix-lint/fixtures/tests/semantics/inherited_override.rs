//! Source-contract compiler fixture; no runtime behavior is asserted.

#[cfg_attr(nervix_lint, nervix::context(recurring, reason = "the caller executes this contract"))]
pub mod task { #[cfg_attr(nervix_lint, nervix::context(bounded, key = "attempt", bound = "one attempt", reason = "the caller executes this contract"))]
pub fn resolve(map: &nervix_lint_fixtures::MapAlias) { drop(map.get(&1)); } }
