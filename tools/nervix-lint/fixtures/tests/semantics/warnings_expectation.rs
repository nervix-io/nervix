//! The warnings group cannot suppress unrelated Nervix rule families.
#[cfg_attr(nervix_lint, nervix::context(recurring, reason = "handles each batch"))]
pub fn run(map: &nervix_lint_fixtures::MapAlias) {
    #[cfg_attr(nervix_lint, expect(warnings, reason = "broad warning group"))]
    let value = map.get(&1);
    drop(value);
}
