//! Widening an operation macro cannot suppress a second acquisition.
#[cfg_attr(nervix_lint, nervix::context(recurring, reason = "batch rows"))]
pub fn batch(map: &nervix_lint_fixtures::MapAlias) {
    nervix_lint_fixtures::expect_lint!(nervix::sync_acquisition, "one reviewed operation", {
        drop(map.get(&1));
        drop(map.get(&2));
    });
}
