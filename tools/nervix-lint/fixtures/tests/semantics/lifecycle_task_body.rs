//! A lifecycle binding owns one installation callback.
#[cfg_attr(nervix_lint, nervix::context(lifecycle, reason = "one installation"))]
pub fn install(map: &nervix_lint_fixtures::MapAlias) {
    #[cfg_attr(nervix_lint, nervix::context(lifecycle, reason = "callback publishes this lifetime"))]
    let callback = || { drop(map.get(&1)); };
    callback();
}
