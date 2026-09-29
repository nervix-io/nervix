//! Concrete branch instance lifetimes for the tasks that own them.
//!
//! Layer: data plane.
//!
//! - **Owns.** Branch instance identity, incarnation assignment, activity order, TTL expiry and
//!   LRU eviction.
//! - **Depends on.** Vocabulary timestamps and indexed collections.
//! - **Must not know.** What a branch instance holds, relays, processors, payloads, metrics,
//!   persistence, NSPL, or control-plane transactions.

mod registry;

pub use registry::{
    BranchInstanceRegistry, BranchInstanceSnapshotEntry, GetOrCreateBranchInstance,
};
