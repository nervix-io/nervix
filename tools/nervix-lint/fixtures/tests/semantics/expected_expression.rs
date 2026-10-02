//! A single acquisition expression carries its own reviewed exception.

#[cfg_attr(nervix_lint, nervix::context(recurring, reason = "the fixture models recurring work"))]
pub fn batch(map: &nervix_lint_fixtures::MapAlias) {
    let value = nervix_lint_fixtures::expect_lint!(nervix::sync_acquisition, "one reviewed expression in the fixture", map.get(&1));
    drop(value);
}
