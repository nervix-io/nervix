//! Source-contract compiler fixture; no runtime behavior is asserted.

#[cfg_attr(nervix_lint, nervix::context(bounded, key = "attempt", bound = "one admitted attempt", reason = "the protocol retains its attempt owner"))]
pub fn resolve(map: &nervix_lint_fixtures::MapAlias) { drop(map.get(&1)); }
