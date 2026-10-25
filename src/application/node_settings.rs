//! Every setting one node runs with, each in the group of its owner.
//!
//! Layer: edges.
//!
//! - **Owns.** The composition of the class-typed settings groups an application carries, and
//!   the join terms its settings make the node bring to the cluster.
//! - **Depends on.** Each owner's settings group, which declares its settings' classes,
//!   defaults and validation.
//! - **Must not know.** Command-line parsing, or how any owner uses its values.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use nervix_consensus::{RAFT_LOG_DURABILITY, RaftRetentionSettings, RaftTimings};
use nervix_interconnect::JoinTerms;
use nervix_models::PrerequisiteTimings;

use super::{
    CommandExecutionPolicy, ShutdownSettings, scheduling::SchedulingSettings,
    transaction::TransactionLimits,
};
use crate::{
    capacity::CapacitySettings, cluster::MembershipSettings,
    memory_pressure::MemoryPressureSettings, resource::ResourceSettings, runtime::RuntimeSettings,
};

/// Every setting one node runs with, each in the group of the owner that declares it.
///
/// Its default resolves every definition's own default; nothing else writes one.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NodeSettings {
    pub raft: RaftTimings,
    pub raft_retention: RaftRetentionSettings,
    pub membership: MembershipSettings,
    pub scheduling: SchedulingSettings,
    pub runtime: RuntimeSettings,
    pub transactions: TransactionLimits,
    pub commands: CommandExecutionPolicy,
    pub memory_pressure: MemoryPressureSettings,
    pub shutdown: ShutdownSettings,
    pub resources: ResourceSettings,
    pub capacity: CapacitySettings,
}

impl NodeSettings {
    /// The prerequisites these settings make the node bring to the cluster beside what its
    /// transport owns: the durability its Raft log promises and its timings.
    pub fn join_terms(&self) -> JoinTerms {
        JoinTerms {
            durability: RAFT_LOG_DURABILITY,
            timings: PrerequisiteTimings {
                raft_heartbeat_interval: self.raft.heartbeat_interval(),
                gossip_interval: self.membership.gossip_interval(),
                clock_skew_tolerance: self.membership.clock_skew_tolerance(),
            },
        }
    }
}
