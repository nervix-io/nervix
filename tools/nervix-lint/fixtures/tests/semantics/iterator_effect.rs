//! Implicit iteration reconsiders the selected local iterator implementation.
struct Rows<'a>(&'a nervix_lint_fixtures::MapAlias);
impl Iterator for Rows<'_> {
    type Item = ();
    fn next(&mut self) -> Option<()> { drop(self.0.get(&1)); None }
}
#[cfg_attr(nervix_lint, nervix::context(recurring, reason = "batch rows"))]
pub fn batch(map: &nervix_lint_fixtures::MapAlias) { for _ in Rows(map) {} }
