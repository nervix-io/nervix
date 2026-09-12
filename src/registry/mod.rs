//! The decisions a cluster makes about Models, before anything runs.
//!
//! Layer: decisions.
//!
//! - **Owns.** The durable model store, validation of domains, references, schemas, branches,
//!   capabilities and execution contracts, transaction mutation planning, placement and relocation,
//!   the active execution graph, and the assignment of nodes to cluster members.
//! - **Depends on.** The vocabulary, the dataflow-graph description, the VM and the UDF host to
//!   type-check what it validates, and `fjall` for storage.
//! - **Must not know.** How a validated node runs. No Tokio task, no Arrow batch, no connector and
//!   no branch-local state belongs here, and a decision must be computable without a cluster.
//!
mod domain_state;
mod error;
mod graph;
mod mutation;
mod placement;
mod relocation;
mod scheduler;
mod storage;
#[cfg(test)]
mod test_fixtures;
mod validation;

/// What the decisions layer exposes. Everything else this module and its submodules declare is
/// `pub(in crate::registry)` or narrower, so the control plane reaches the registry only through
/// the names below.
pub(crate) use error::RegistryError;
pub(crate) use graph::{ActiveGraph, EdgeKind};
pub(crate) use mutation::{PlannedMutations, RegistryMutation};
pub(crate) use placement::{
    PlacementEndpointPairPlan, PlacementPlan, PlacementRequireGroupPlan, PlacementRulePlan,
};
pub(crate) use relocation::{RelocationCoverage, RelocationMemberReason, RelocationUnit};
#[cfg(feature = "testing")]
pub use scheduler::SchedulerMode;
pub(crate) use storage::{Registry, RuntimeChange, RuntimeChanges};
