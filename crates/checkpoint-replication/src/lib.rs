//! The replication of one runtime-state placement's checkpoints, as one node takes part in it: as
//! the placement's owner, what each replica reported holding and the offer of the newest checkpoint
//! to the replicas that do not hold it yet; as a replica, the owner's announcements of newer
//! checkpoints.
//!
//! Layer: data plane.
//!
//! - **Owns.** Each replica's highest reported revision, which never falls; the revision the owner
//!   offers and the one announcer offering it; waiting for replicas to hold a revision without
//!   missing a report; and waking a replica's synchronization when its owner announces.
//! - **Depends on.** Cluster node names and the primitive synchronization boundary.
//! - **Must not know.** Runtime-state placements, what a checkpoint holds, how it is announced,
//!   fetched, installed or persisted, schedules, the interconnect, or NSPL.

mod progress;
mod replication;

pub use progress::ReplicaProgress;
pub use replication::{Announcer, AnnouncerStep, CheckpointReplication};
