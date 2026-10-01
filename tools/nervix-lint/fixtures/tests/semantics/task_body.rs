//! A task installed once has a separately recurring anonymous body.
#[cfg_attr(nervix_lint, nervix::context(lifecycle, reason = "task installation"))]
pub fn install(map: &nervix_lint_fixtures::MapAlias) {
    #[cfg_attr(nervix_lint, nervix::context(recurring, reason = "installed task handles batches"))]
    let task = || { drop(map.get(&1)); };
    task();
}
