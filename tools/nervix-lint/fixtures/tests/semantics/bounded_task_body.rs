//! A retained task body can document its bounded operation protocol.
#[cfg_attr(nervix_lint, nervix::context(lifecycle, reason = "task installation"))]
pub fn install(map: &nervix_lint_fixtures::MapAlias) {
    #[cfg_attr(nervix_lint, nervix::context(bounded, reason = "retained branch admission", key = "branch generation", bound = "one terminal transition"))]
    let task = || { drop(map.get(&1)); };
    task();
}
