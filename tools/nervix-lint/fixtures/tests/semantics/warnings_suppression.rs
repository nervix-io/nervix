//! Rust's warnings group cannot conceal architecture diagnostics.
#![allow(warnings)]
#[cfg_attr(nervix_lint, nervix::context(recurring, reason = "batch rows"))]
pub fn batch(map: &nervix_lint_fixtures::MapAlias) { drop(map.get(&1)); }
