//! Source-contract compiler fixture; no runtime behavior is asserted.

#[cfg_attr(nervix_lint, nervix::context(recurring, reason = "the caller executes this contract"))]
pub trait Apply { fn apply(&self); }
pub struct State(pub nervix_lint_fixtures::MapAlias);
impl Apply for State { fn apply(&self) { drop(self.0.get(&1)); } }
