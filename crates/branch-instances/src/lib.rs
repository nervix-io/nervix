//! Concrete branch instance lifetimes for the tasks that own them, and the branch membership an
//! owner publishes for its observers.
//!
//! Layer: data plane.
//!
//! - **Owns.** Branch instance identity, incarnation assignment, activity order, TTL expiry and
//!   LRU eviction, and the immutable branch membership one owner publishes whenever it changes.
//! - **Depends on.** Vocabulary timestamps, the primitive publication boundary, and indexed and
//!   persistent collections.
//! - **Must not know.** What a branch instance holds, relays, processors, payloads, metrics,
//!   persistence, NSPL, or control-plane transactions.

mod membership;
mod registry;

pub use membership::{BranchAdmission, BranchMembership, BranchPresence, OwnedBranches};
pub use registry::{
    BranchInstanceRegistry, BranchInstanceSnapshotEntry, GetOrCreateBranchInstance,
};

// Counts allocations per thread, so the tests can hold admission to the allocations a membership
// change actually needs.
#[cfg(test)]
#[global_allocator]
static ALLOCATOR: alloc_count::AllocCounter = alloc_count::AllocCounter(std::alloc::System);
