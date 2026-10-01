//! A module default cannot conceal a helper called by a recurring entry point.
#![cfg_attr(nervix_lint, nervix::context(lifecycle, reason = "installation module default"))]

fn helper(map: &nervix_lint_fixtures::MapAlias) { drop(map.get(&1)); }
#[cfg_attr(nervix_lint, nervix::context(recurring, reason = "one invocation per batch"))]
pub fn batch(map: &nervix_lint_fixtures::MapAlias) { helper(map); }
