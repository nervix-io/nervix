//! How a model check of a production owner is explored, and the evidence each exploration leaves.
//!
//! A model check drives a production owner from a few threads of a model checker, which runs the
//! program once for every schedule it can tell apart. What a passing check proves depends on how
//! far that search went, so the bounds belong to the check, a run reports them, and a setting that
//! would stop the search early is refused rather than turned into a partial success.
//!
//! Loom models and Shuttle checks are the ones this crate runs. A Loom model names the invariant it
//! checks with an [`InvariantId`], which `just test-loom` matches against the model inventory in
//! `crates/model-harness/loom-inventory.toml`, and `loom::explore` explores it to exhaustion
//! with no preemption bound. A Shuttle check is named by its test, which `just test-shuttle` matches
//! against the inventory in `crates/model-harness/shuttle-inventory.toml`, and the `shuttle` module
//! explores it under the schedulers and bounds the check chooses, fails it when a search ends early,
//! and persists and replays its failing schedules. The `loom` and `shuttle` features select that
//! checker's primitives for the whole dependency graph through `nervix-primitives`; without them
//! the crate offers only the identity and the settings policy, so an ordinary workspace build never
//! contains a model checker.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** The bounds a model or check is explored under, refusing settings that would end a
//!   search early, and the record a run prints when its search completes.
//! - **Depends on.** Loom's model runner, Shuttle's runner and schedulers, the primitive boundary
//!   that selects either for the graph, and runner statistics kept outside the model.
//! - **Must not know.** Which owner a model drives or what its invariant claims.

mod exploration;
#[cfg(feature = "loom")]
pub mod loom;
#[cfg(feature = "shuttle")]
pub mod shuttle;

pub use exploration::{
    InvariantId, LOOM_BRANCH_LIMIT, REFUSED_LOOM_SETTINGS, refused_loom_settings,
};
