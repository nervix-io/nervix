//! Recurring execution cannot conceal an installation behind an anonymous body.
#[cfg_attr(nervix_lint, nervix::context(recurring, reason = "handles each batch"))]
pub fn run(map: &nervix_lint_fixtures::MapAlias) {
    #[cfg_attr(nervix_lint, nervix::context(lifecycle, reason = "callback publishes this lifetime"))]
    let callback = || { drop(map.get(&1)); };
    callback();
}
